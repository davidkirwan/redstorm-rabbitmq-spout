require 'red_storm'
require './redstorm_rabbitmq_spout'


class ExampleTopology < RedStorm::DSL::Topology
  
###############################################
  spout RedstormRabbitmqSpout, :parallelism => 1 do
    output_fields :datablock
  end

  configure do |env|
    debug false
    max_task_parallelism 1
    num_workers 2
    max_spout_pending 1000
  end
  
  on_submit do |env|
    if env == :local
      sleep(70)
      cluster.shutdown
    end
  end
end

