# frozen_string_literal: true

module DurableFlow
  module JobReference
    module_function

    def run_id_for(job_or_run_id)
      job_or_run_id.respond_to?(:job_id) ? job_or_run_id.job_id : job_or_run_id.to_s
    end

    def class_name_for(job_class)
      job_class.respond_to?(:name) ? job_class.name : job_class.to_s
    end
  end
end
