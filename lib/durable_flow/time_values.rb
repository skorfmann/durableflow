# frozen_string_literal: true

module DurableFlow
  module TimeValues
    PRECISION = 9

    module_function

    def format(time)
      time.utc.iso8601(PRECISION)
    end

    def parse(value)
      return if value.blank?
      return value if value.is_a?(Time)

      Time.iso8601(value.to_s)
    end

    def from(value, explicit_time: nil)
      return explicit_time.to_time if explicit_time.respond_to?(:to_time)
      return nil if value.nil?
      return value.to_time if value.respond_to?(:to_time)

      Time.current + value
    end
  end
end
