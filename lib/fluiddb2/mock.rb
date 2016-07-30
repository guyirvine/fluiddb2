require 'FluidDb2'

module FluidDb2
  class SqlNotMatchedError < StandardError
  end

  # A constant way of enabling testing for FluidDb
  class Mock < Base
    attr_reader :hash

    def initialize(_uri)
      @hash = {}
      @verbose = false
    end

    def verbose
      @verbose = true
      self
    end

    def connect
    end

    def close
    end

    def get_sql_from_hash(sql)
      fail SqlNotMatchedError, sql unless @hash.key?(sql)

      @hash[sql]
    end

    def query_for_array(sql, params = [])
      sql = format_to_sql(sql, params)
      puts "FluidDb::Mock.query_for_array. sql: #{sql}" if @verbose == true

      results = get_sql_from_hash(sql)
      case results.length
      when 0
        fail FluidDb::NoDataFoundError
      when 1
        return results.first
      else
        fail FluidDb::TooManyRowsError
      end
    end

    def query_for_value(sql, params = [])
      sql = format_to_sql(sql, params)
      puts "FluidDb::Mock.queryForValue. sql: #{sql}" if @verbose == true

      results = get_sql_from_hash(sql)
      case results.length
      when 0
        fail FluidDb::NoDataFoundError
      when 1
        return results.first.first[1]
      else
        fail FluidDb::TooManyRowsError
      end
      @hash[sql]
    end

    def query_for_resultset(sql, params = [])
      sql = format_to_sql(sql, params)
      puts "FluidDb::Mock.queryForResultset. sql: #{sql}" if @verbose == true
      get_sql_from_hash(sql)
    end

    def execute(sql, params = [], _expected_affected_rows = nil)
      sql = format_to_sql(sql, params)
      puts "FluidDb::Mock.execute. sql: #{sql}" if @verbose == true
      get_sql_from_hash(sql)
    end

    def insert(_sql, _params)
      fail 'Mock uses SEQUENCES, so possibly easier to use 2 executes'
    end

    def add_sql(sql, result)
      raise TypeError.new( "Expecting an Array of Hashes, eg [{'field1'=>1, 'field2'=>2}]. Note, the Array may be empty" ) unless result.is_a? Array

      @hash[sql] = result
    end

    def add_sql_with_params(sql, params, result)
      sql = format_to_sql(sql, params)
      add_sql(sql, result)
    end

    # Transaction Semantics
    def begin
    end

    # Transaction Semantics
    def commit
    end

    # Transaction Semantics
    def rollback
    end
  end
end
