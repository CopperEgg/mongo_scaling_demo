#!/usr/bin/env ruby

require 'rubygems'
require 'mongo'

mongo = Mongo::Connection.new
collection = mongo.db('app').collection('users')
collection.remove()
usernames = []
open('surnames.txt') do |file|
  while !file.eof?
    surname = file.gets.downcase.gsub(/\s/, '')
    ('a'..'z').each do |letter|
      usernames << "#{letter}_#{surname}"
    end
  end
end

begin
  collection.drop_index('sleep_until_-1_reserved_at_1_last_processed_-1')
  collection.drop_index('reserved_at_1')
rescue Exception => e
  # do nothing
end

# This randomizes all the usernames so they are not in alphabetical order when
# in mongo, just to make it look more realistic.
usernames.sort {|a,b| rand <=> 0.5}.each do |username|
  collection.insert({:_id=>username,
                     :reserved_at=>0,
                     :sleep_until=>0,
                     :last_processed=>0,
                     :counter=>0})
end

collection.ensure_index([[:sleep_until,-1],[:reserved_at,1],[:last_processed,-1]])
collection.ensure_index([[:reserved_at,1]])
