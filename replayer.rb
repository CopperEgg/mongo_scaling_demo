#!/usr/bin/env ruby

require 'rubygems'
require 'mongo'
require 'pp'

def import_data(filename)
  mongo = Mongo::Connection.new
  collection = mongo.db('weather').collection('queue')

  File.open(filename) do |f|
    f.gets
    while !f.eof? do
      line = f.gets.split(/ +/)
      collection.insert({
          :reserved_at => 0,
          :location => line[0].to_i,
          :date => line[2].to_i,
          :temp => line[3].to_f,
          :dewp => line[5].to_f,
          :slp => line[7].to_f,
          :stp => line[9].to_f,
          :visib => line[11].to_f,
          :wdsp => line[13].to_f,
          :mxspd => line[15].to_f,
          :max => line[17].to_f,
          :min => line[18].to_f
        })
      sleep 1
    end
  end
end

def setup
  mongo = Mongo::Connection.new
  mongo.drop_database('weather')
  mongo.db('weather').collection('queue').create_index([[:reserved_by,1]])
end

setup
num_threads = ARGV[0].to_i
files = Dir.glob("sample_data/*.op").sort {|a,b| rand <=> 0.5}
threads = []
num_threads.times do |i|
  threads << Thread.new { import_data(files[i]) }
end
threads.each {|thread| thread.join}
