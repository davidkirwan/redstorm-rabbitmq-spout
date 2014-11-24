require 'red_storm'
require 'json'
require 'bunny'


class RedstormRabbitmqSpout < RedStorm::DSL::Spout
  output_fields :datablock
  
  on_send do
    begin
      unless @datablocks.size == 0
        payload = @datablocks.pop
      else
        pop_messages()
        payload = @datablocks.pop
      end
      
      if payload == "" or payload.class == NilClass 
        raise Exception, "Payload empty, or Payload nil"
      else

        payload
      end
      
    rescue Exception => e
      on_close
    end
  end
  
  
  on_close do
    retries = 3
    begin
      @q.delete
    rescue
      retries -= 1
      if retries > 0
        retry
      end
    end
  end
  

  on_init do
    unless @datablocks.class == Array then @datablocks = Array.new; end
    
    @options = {
      :serverport=>2000,
      :mbhost=>"10.0.0.0",
      :mbport=>5672,
      :mbuser=>'guest',
      :mbpassword=>'guest',
      :topic=>'the.topic.name',
      :queue=>'the.queue.name',
      :binding_key=>'the.binding.key'
    }
    
    # Setup the MB connection
    @connection = makeConnection(@options)
    
    @ch = @connection.create_channel
    @x = @ch.topic(@options[:topic])
    @ch.prefetch(100)
    @q = @ch.queue(@options[:queue], :exclusive => false, :durable=>false, :auto_delete => true)
    @q.bind(@x, :routing_key=>@options[:binding_key])
    
    pop_messages()
  end

  def pop_messages()
    unless @q.message_count < 100
      100.times do
        delivery_info, properties, payload = @q.pop(:auto_ack=>true)
        unless payload.nil? then @datablocks.push(payload); end
      end
    else
      0.upto(@q.message_count) do
        delivery_info, properties, payload = @q.pop(:auto_ack=>true)
        unless payload.nil? then @datablocks.push(payload); end
      end
    end
  end
  
  def makeConnection(options)
    @options = options
    unless @rabbit.nil? then return @rabbit; end
          
    @rabbit = Bunny.new(:host=>@options[:mbhost],
                        :port=>@options[:mbport],
                        :user=>@options[:mbuser],
                        :password=>@options[:mbpassword])

    @rabbit.start # Start the connection
          
    return @rabbit
  end

# End of the RedstormRabbitmqSpout class
end

