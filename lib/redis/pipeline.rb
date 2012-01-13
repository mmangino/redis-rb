class Redis
  class Pipeline
    attr :commands
    attr :blocks

    def initialize
      @without_reconnect = false
      @shutdown = false
      @commands = []
      @blocks = []
    end

    def without_reconnect?
      @without_reconnect
    end

    def shutdown?
      @shutdown
    end

    # Starting with 2.2.1, assume that this method is called with a single
    # array argument. Check its size for backwards compat.
    def call(*args, &block)
      if args.first.is_a?(Array) && args.size == 1
        command = args.first
      else
        command = args
      end

      # A pipeline that contains a shutdown should not raise ECONNRESET when
      # the connection is gone.
      @shutdown = true if command.first == :shutdown
      @commands << command
      proxy = PipelineResult.new
      @blocks << Proc.new do |result| 
        return_value = block.nil? ? result : block.call(result)
        proxy.result = return_value
        return_value
      end 
      proxy
    end

    def call_pipeline(pipeline, options = {})
      @shutdown = true if pipeline.shutdown?
      @commands.concat(pipeline.commands)
      @blocks.concat(pipeline.blocks)
      nil
    end

    def without_reconnect(&block)
      @without_reconnect = true
      yield
    end
    class UnexecutedRequest < StandardError; end

    class PipelineResult
      instance_methods.each { |m| undef_method m unless m =~ /(^__|^nil\?$|^send$|proxy_|^respond_to\?$|^new|object_id$)/ }
      def initialize
        @result     = nil
        @result_set = false
      end

      def result=(result_object)
        @result_set = true
        @result = result_object
      end


      def respond_to?(name)
        super || @result.respond_to?(name)
      end

      def ===(other)
        other === @result
      end

      def method_missing(name,*args,&proc)
        if !@result_set
          raise UnexecutedRequest.new("You may not access the result until the pipeline has been executed")
        else
          @result.send(name,*args,&proc)
        end
      end
    end
  end
end
