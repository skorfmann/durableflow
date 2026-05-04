# frozen_string_literal: true

module DurableFlow
  module Serializer
    module_function

    def dump(value)
      ActiveJob::Arguments.serialize([ value ]).first
    end

    def load(value)
      ActiveJob::Arguments.deserialize([ value ]).first
    end
  end
end
