#!/usr/bin/ruby1.9.1
# -*- coding: utf-8 -*-

require 'fileutils'
require 'net/http'
require 'json'
require 'oauth'
require 'thread'
require 'grit'
require 'yaml'

KEY='M6gi0rr1VfYhjKbSfqEzA'
SEC='VGcOQGZUdsBa9Ar87fsu7wnHqE75n912lDneileXyY'
USERSTREAM='https://userstream.twitter.com/1.1/user.json'

@queue = Queue.new
@repo  = nil

trap(:INT) {
  exit
}

def write_thread
  loop do
    begin
      user_data, commit = @queue.pop
      write_user_data(user_data, commit)
    rescue
      puts $!
    end
  end
end

def write_user_data(user_data, commit = false)
  _id = user_data['id']
  return if  _id.nil?
  text = <<EOF
screen_name: #{user_data['screen_name']}
name: #{user_data['name']}
description: #{user_data['description']}
location: #{user_data['location']}
EOF
  File.open("ids/#{_id}", "w+") do |io|
    io.write text
  end
  FileUtils.ln_s("../ids/#{_id}", "screen_name/#{user_data['screen_name']}", { :force => true })
  system('find -L screen_name/ -type l -exec rm {} \;')
  if commit
    @repo.add("ids/#{_id}")
    message = <<EOF
Update ID #{_id}

 - screen_name: #{user_data['screen_name']}
 - name: #{user_data['name']}
 - description: #{user_data['description']}
 - location: #{user_data['location']}
EOF
    ret = @repo.commit_index( message )
    unless /^#/ =~ ret
      print "Commit: Update #{_id} ( #{user_data["screen_name"]} )\n"
    end
  end
end

def streaming
  uri = URI::parse( USERSTREAM )
  http = Net::HTTP.new( uri.host, uri.port )
  http.use_ssl = true if uri.scheme == 'https'
  request_uri = uri.request_uri
  request = Net::HTTP::Get.new( request_uri )
  cons  = OAuth::Consumer.new( KEY, SEC, { :site=>"https://api.twitter.com/ " } )
  token = OAuth::AccessToken.new( cons, @config[:access_token], @config[:access_token_secret] )
  #response = token.get('/1.1/account/verify_credentials.json')
  #json = JSON::parse( response.body )
  #branch = json['id_str']
  #print "use branch '#{branch}'\n"
  #@repo.checkout( branch )
  request.oauth!( http, cons, token )
  print "Stalking start\n"
  begin
    buf = ''
    thread = nil
    http.request( request ) do |res|
      if res.code != "200"
        puts res.body
        raise RuntimeError, "Error on HTTP HTTP:#{res.code} #{res.to_s}" if res.code.to_i != 200
      end
      res.read_body do |str|
        buf << str
        buf.gsub!( /[\s\S]+?\r\n/ ) do |chunk|
          json = JSON::parse( chunk ) rescue next
          if json['friends']
            thread = Thread.start {
              ids_added = []
              json['friends'].each do |_id|
                unless File.exist?( "ids/#{_id}" )
                  begin
                    sleep 0.5
                    response = token.get( "/1.1/users/show.json?user_id=#{_id}" )
                    next unless response.code.to_i == 200
                    user_data = JSON::parse( response.body )
                    @queue.push( [ user_data, false ] )
                    ids_added.push( _id )
                    break if response.header['X-Rate-Limit-Remaining'].to_i < 10
                  rescue
                    print "#{Time.now.to_s}\n"
                    print "#{$!}\n"
                    print "#{$!.backtrace.join("\n")}\n"
                  end
                end
              end
              @repo.add( 'ids' )
              @repo.commit_index( "Add #{ids_added.size} ids\n\n - Added: #{ids_added.join(', ')}\n" )
            }
            next
          end
          next if json.has_key?( 'retweeted_status' )
          next unless json.has_key?( 'user' )
          user_data = json['user']
          @queue.push( [ user_data, true ] ) unless ( thread and thread.alive? )
          nil
        end
      end
    end
    puts "done http"
  rescue
    print "#{Time.now.to_s}\n"
    print "#{$!}\n"
    print "#{$!.backtrace.join("\n")}\n"
  ensure
    http.finish rescue nil
  end
end

def init()
  Dir.chdir(File.dirname(__FILE__))
  @repo = Grit::Repo.init('.')
  FileUtils.mkdir_p('ids')
  FileUtils.mkdir_p('screen_name')
  @config = YAML.load_file('.token')
end

init
Thread.start { write_thread }

loop do
  begin
    streaming
  rescue
    print "#{Time.now.to_s}\n"
    print "#{$!}\n"
    print "#{$!.backtrace.join("\n")}\n"
    sleep 5
    retry
  end
end
