#!/usr/bin/env ruby

require 'rubygems'
require 'mongo'

class Summarizer
  def initialize
    @interrupted = false
    @mongo = Mongo::Connection.new
  end

  ##########
  # This is the main loop of the daemon.  We have two threads running, one to
  # do the processing (which will consume most of the CPU) and one to
  # periodically do housekeeping but sleeps most of the time.
  def run
    # Caputre these two signals so if you hit CTRL+C or kill the daemon, it
    # will exit cleanly.  Setting @interrputed to true tells our threads to
    # finish what they're working on and then exit.
    trap("INT") { @interrupted = true; puts " Exiting cleanly..." }
    trap("TERM") { @interrupted = true; puts " Exiting cleanly..." }

    threads = []
    threads << Thread.new { housekeeping }
    threads << Thread.new { process_data }

    # Now we join both threads.  This will essentially block until both threads
    # have exited cleanly.
    threads.each {|thread| thread.join }

    log "Exited cleanly"
  end

  ##########
  # This loop is run in a thread from the run method.  It sleeps most of the
  # time but wakes up periodically to do some housekeeping.
  def housekeeping
    while !@interrupted
      release_stale_reservations
      watchful_sleep 30
    end
  end

  ##########
  # This loop is run in a thread from the run method.  It does the bulk of our
  # work.  It checks to see if there is work to do, makes a reservation, does
  # the work, then releases the reservation.  It constantly does this until
  # there is no more work to do, then sleeps for a bit.
  def process_data
    while !@interrupted
      uid = reserve_user
      if uid
        starttime = Time.now.to_f
        sleep rand/1000 # This is where you'll do your actual work.
        duration = ((Time.now.to_f - starttime) * 10000).round / 10000.0
        log "Summarized #{uid} (#{duration}s)"
        release_reservation(uid)
      else
        log "No work to do.  Sleeping 3 seconds."
        watchful_sleep 3
      end
    end
  end

  ##########
  # Atomically makes a reservation in MongoDB for the next user that needs to
  # be processed.  A user id is returned representing the user we have a
  # reservation on.  If no user needs processed right now, nil is returned.
  def reserve_user
    doc = @mongo.db('app').collection('users').find_and_modify(
        :query=>{:sleep_until=>{:$lt=>Time.now.to_i}, :reserved_at=>0},
        :sort=>[[:last_processed,-1]],
        :update=>{:$set=>{:reserved_at=>Time.now.to_i}})
    return nil if !doc
    return doc['_id']
  end

  ##########
  # Takes a user id representing a reservation you have taken (returned from
  # reserve_user) and removes the reservation so this user can be processed
  # later.  It also sets the sleep_until time in the future so we don't sit in
  # a tight loop constantly spinning on the same site with no work to do.
  def release_reservation(uid)
    @mongo.db('app').collection('users').update(
        {:_id=>uid},
        {:$set=>{:reserved_at=>0,
                 :last_processed=>Time.now.to_i,
                 :sleep_until=>Time.now.to_i+5}})
  end

  ##########
  # We need to periodically check for stale reservations, because a daemon could
  # crash before it releases its reservation.  We want that user to eventually be
  # processed so after a sufficient amount of time, we will release the user so
  # it can be processed again.  The timeout should be long enough that any work
  # should have been done already, but short enough that there won't be a
  # noticeable delay to the end user.
  def release_stale_reservations
    res = @mongo.db('app').collection('users').update(
        {:reserved_at=>{:$lt=>Time.now.to_i-30, :$gt=>0}},
        {:$set=>{:reserved_at=>0}},
        {:multi=>true, :safe=>true});
    log "Released #{res['n']} stale reservations."
  end

  ##########
  # Now we want to sleep for a while so this doesn't sit in a tight loop.
  # But if we have been interrupted with a KILL or INT signal, we want to
  # exit cleanly as quickly as possible
  def watchful_sleep(seconds)
    seconds.times { sleep 1 if !@interrupted }
  end

  ##########
  # A utility method to standardize the logging output.
  def log(str)
    puts "#{Time.now.strftime("%Y/%m/%d %H:%M:%S")}> #{str}"
  end
end


# If run from the command line, execute the run method
if __FILE__ == $0
  Summarizer.new.run
end
