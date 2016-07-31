require 'fluiddb2'
require 'tiny_tds'
require 'cgi'

module FluidDb2
  class TinyTds < Base

    # Connect to Db.
    #
    # @param [String] uri a location for the resource to which we will attach,
    #  eg tinytds://<user>:<pass>@<dataserver>/<database>
    def connect
      uri = @uri

      dataserver = uri.host
      database = uri.path.sub('/', '')
      username = URI.unescape(uri.user)
      password = uri.password

      if dataserver == '' || database == ''
        fail '*** You need to specify both a dataserver and a database for ' \
              'the tinytds driver. Expected format: ' \
              'tinytds://<user>:<pass>@<dataserver>/<database>\n' \
              '*** The specified dataserver should have an entry in ' \
              '/etc/freetds/freetds.conf'
      end

      if username == '' || password == ''
        puts '*** Warning - you will normally need to specify both a username ' \
             'and password for the tinytds driver to work correctly.'
      end

      hash = Hash[:username, username,
                  :password, password,
                  :database, database,
                  :dataserver, dataserver]
      fail_over_data_server = nil
      unless uri.query.nil?
        cgi = CGI.parse(uri.query)
        hash[:timeout] = cgi['timeout'][0].to_i if cgi.key?('timeout')
        if cgi.key?('host')
          hash.delete(:dataserver)
          hash[:host] = dataserver
        end

        fail_over_data_server =
          cgi['failoverdataserver'][0] if cgi.key?('failoverdataserver')
      end

      begin
        @connection = ::TinyTds::Client.new(hash)
      rescue ::TinyTds::Error => e
        # Message for an incorrect password,
        # Login failed. The login is from an untrusted domain and cannot be used
        #  with Windows authentication.
        # Message for unavailable db
        # Cannot open user default database. Login failed.
        if e.message == 'Cannot open user default database. Login failed.' &&
           !fail_over_data_server.nil?
          hash[:dataserver] = fail_over_data_server
          @connection = ::TinyTds::Client.new(hash)
        else
          raise e
        end
      end

      fail 'Unable to connect to the database' unless @connection.active?
    end

    def close
      @connection.close
    end

    def escape_string(input)
      @connection.escape(input)
    end

    def query_for_array(sql, params = [])
      sql = FluidDb2.format_to_sql(sql, params)
      results = @connection.execute(sql)

      count = 0
      tuple = ''
      results.each do |row|
        count += 1
        fail FluidDb2::TooManyRowsError if count > 1
        tuple = row
      end
      fail FluidDb2::NoDataFoundError if count == 0
      tuple
    end

    def query_for_value(sql, params = [])
      sql = FluidDb2.format_to_sql(sql, params)
      results = @connection.execute(sql)

      count = 0
      value = ''
      results.each do |row|
        count += 1
        fail FluidDb2::TooManyRowsError if count > 1
        value = row[results.fields[0]]
      end

      fail FluidDb2::NoDataFoundError if count == 0

      value
    end

    def query_for_resultset(sql, params = [])
      sql = FluidDb2.format_to_sql(sql, params)
      results = @connection.execute(sql)

      list = []
      results.each do |row|
        list << row
      end

      list
    end

    def execute(sql, params = [], expected_affected_rows = nil)
      sql = FluidDb2.format_to_sql(sql, params)
      r = @connection.execute(sql)
      r.each

      if !expected_affected_rows.nil? &&
         r.affected_rows != expected_affected_rows
        msg = "Expected affected rows, #{expected_affected_rows}, " \
              "Actual affected rows, #{r.affected_rows}"
        fail ExpectedAffectedRowsError, msg
      end
    end

    def insert(_sql, _params)
      fail 'Not implemented'
    end

    # Transaction Semantics
    def begin
      @connection.execute('BEGIN')
    end

    # Transaction Semantics
    def commit
      @connection.execute('COMMIT')
    end

    # Transaction Semantics
    def rollback
      @connection.execute('ROLLBACK')
    end
  end
end
