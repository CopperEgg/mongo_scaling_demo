#!/usr/bin/env ruby

require 'rubygems'
require 'bundler'
Bundler.require(:default) if defined?(Bundler)

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
    # Capture these two signals so if you hit CTRL+C or kill the daemon, it
    # will exit cleanly.  Setting @interrupted to true tells our threads to
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
      doc = reserve_item
      if doc
        starttime = Time.now.to_f

        # update the summary data
        @mongo.db('weather').collection('summaries').update(
              {:_id=>doc['location']},
              {:$inc=>{:avg_temp_total=>doc['temp'],
                      :avg_temp_count=>1,
                      :avg_dewp_total=>doc['dewp'],
                      :avg_dewp_count=>1}},
              {:upsert=>true});
        # sleep for a little bit to simulate a workload
        sleep 0.1 + rand/25

        duration = ((Time.now.to_f - starttime) * 10000).round / 10000.0
        log "Processed => location:#{doc['location']}, date:#{doc['date']} (#{duration} seconds)"
        release_reservation(doc)
      else
        log "No work to do.  Sleeping 1 second."
        watchful_sleep 1
      end
    end
  end

  ##########
  # Atomically makes a reservation in MongoDB for the next item that needs to
  # be processed.  The item's document is returned representing the item we have a
  # reservation on.  If nothing needs processed right now, nil is returned.
  def reserve_item
    doc = @mongo.db('weather').collection('queue').find_and_modify(
        :query=>{:reserved_at=>0},
        :update=>{:$set=>{:reserved_at=>Time.now.to_i}})
    return nil if !doc
    return doc
  end

  ##########
  # Takes a document representing a reservation you have taken (returned from
  # reserve_item) and removes it from the queue.
  def release_reservation(doc)
    @mongo.db('weather').collection('queue').remove({:_id=>doc['_id']})
  end

  ##########
  # We need to periodically check for stale reservations, because a daemon could
  # crash before it releases its reservation.  We want that item to eventually be
  # processed so after a sufficient amount of time, we will release the item so
  # it can be processed again.  The timeout should be long enough that any work
  # should have been done already, but short enough that there won't be a
  # noticeable delay to the end user.
  def release_stale_reservations
    res = @mongo.db('weather').collection('queue').update(
        {:reserved_at=>{:$lt=>Time.now.to_i-30}},
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
