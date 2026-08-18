# frozen_string_literal: true

module DurableFlow
  module ErrorPayload
    BACKTRACE_LIMIT = 10

    module_function

    def for(error)
      {
        "class" => error.class.name,
        "message" => error.message,
        "backtrace" => Array(error.backtrace).first(BACKTRACE_LIMIT),
      }
    end
  end
end
