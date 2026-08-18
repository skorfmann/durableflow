# frozen_string_literal: true

require "test_helper"

class DurableFlowSerializerTest < DurableFlowTestCase
  test "round trips primitive values" do
    assert_nil round_trip(nil)

    [ true, false, 1, 1.5, "text" ].each do |value|
      assert_equal value, round_trip(value)
    end
  end

  test "round trips hashes with symbol keys" do
    value = { token: "abc", nested: { count: 2 } }

    assert_equal value, round_trip(value)
  end

  test "round trips times and symbols" do
    time = Time.utc(2024, 1, 2, 3, 4, 5)

    assert_equal time, round_trip(time)
    assert_equal :pending, round_trip(:pending)
  end

  test "round trips nested collections" do
    value = { items: [ 1, { name: "a" } ], flags: [ true, nil ] }

    assert_equal value, round_trip(value)
  end

  test "dump raises for unsupported values" do
    assert_raises(ActiveJob::SerializationError) { DurableFlow::Serializer.dump(Object.new) }
  end

  private
    def round_trip(value)
      DurableFlow::Serializer.load(DurableFlow::Serializer.dump(value))
    end
end
