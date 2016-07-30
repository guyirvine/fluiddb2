require 'minitest/autorun'
require './lib/fluiddb2/mysql2'

# Mysql2SQLTest
class Mysql2SQLTest < Minitest::Test
  def setup
    cs = 'mysql2://vagrant:password@localhost/test'
    @db = FluidDb2::Mysql2.new(URI.parse(cs))
    @db.execute('DROP TABLE table1')
    @db.execute('CREATE TABLE table1 ( id BIGINT NOT NULL AUTO_INCREMENT, ' \
                'field1 BIGINT, field2 VARCHAR(50), PRIMARY KEY (id) );')

    @db.execute("INSERT INTO table1 ( field1, field2 ) VALUES ( 1, 'Two' );")
    @db.execute("INSERT INTO table1 ( field1, field2 ) VALUES ( 2, 'Three' );")
  end

  def test_query_for_array
    sql_in = 'SELECT field1, field2 FROM table1 WHERE field1 = 1'
    r = @db.query_for_array(sql_in)
    assert_equal Hash['field1', 1, 'field2', 'Two'], r
  end

  def test_query_for_array_too_many_rows
    error_raised = false
    sql_in = 'SELECT field1, field2 FROM table1'
    begin
      @db.query_for_array(sql_in)
    rescue FluidDb2::TooManyRowsError
      error_raised = true
    end
    assert_equal true, error_raised
  end

  def test_query_for_value
    sql_in = 'SELECT field2 FROM table1 WHERE field1 = 1'
    field1 = @db.query_for_value(sql_in)
    assert_equal 'Two', field1
  end

  def test_query_for_value_no_data_found
    error_raised = false
    sql_in = 'SELECT field1, field2 FROM table1 WHERE field1 = ?'
    begin
      @db.query_for_value(sql_in, [-1])
    rescue FluidDb2::NoDataFoundError
      error_raised = true
    end
    assert_equal true, error_raised
  end

  def test_query_for_resultset
    sql_in = 'SELECT field1, field2 FROM table1 WHERE field1 > ?'
    resultset = @db.query_for_resultset(sql_in, [0])
    assert_equal [Hash['field1', 1, 'field2', 'Two'], Hash['field1', 2, 'field2', 'Three']], resultset
  end

  def test_delete
    @db.execute('DELETE FROM table1 WHERE field1 = ?', [1])
    sql = 'SELECT count(*) FROM table1 WHERE field1 > ?'
    count = @db.query_for_value(sql, [0])
    assert_equal 1, count.to_i
  end

  def test_update_without_expected_affected_rows
    @db.execute('UPDATE table1 SET field2 = ? WHERE field1 = ?', ['One', 1])
    sql = 'SELECT field2 FROM table1 WHERE field1 = ?'
    field2 = @db.query_for_value(sql, [1])
    assert_equal 'One', field2.to_s
  end

  def test_update_with_correct_expected_affected_rows
    @db.execute('UPDATE table1 SET field2 = ? WHERE field1 = ?', ['One', 1], 1)
    sql = 'SELECT field2 FROM table1 WHERE field1 = ?'
    field2 = @db.query_for_value(sql, [1])
    assert_equal 'One', field2.to_s
  end

  def test_update_with_incorrect_expected_affected_rows
    error_raised = false
    begin
      sql = 'UPDATE table1 SET field2 = ? WHERE field1 = ?'
      @db.execute(sql, ['One', 1], 2)
    rescue FluidDb2::ExpectedAffectedRowsError
      error_raised = true
    end
    sql = 'SELECT field2 FROM table1 WHERE field1 = ?'
    field2 = @db.query_for_value(sql, [1])
    assert_equal 'One', field2.to_s
    assert_equal true, error_raised
  end

  def test_update_with_correct_expected_matched_rows
    @db.execute('UPDATE table1 SET field2 = ? WHERE field1 = ?', ['Two', 1], 1)
    sql = 'SELECT field2 FROM table1 WHERE field1 = ?'
    field2 = @db.query_for_value(sql, [1])
    assert_equal 'Two', field2.to_s
  end

  def test_update_with_incorrect_expected_matched_rows
    error_raised = false
    begin
      sql = 'UPDATE table1 SET field2 = ? WHERE field1 = ?'
      @db.execute(sql, ['Two', 1], 2)
    rescue FluidDb2::ExpectedAffectedRowsError
      error_raised = true
    end
    sql = 'SELECT field2 FROM table1 WHERE field1 = ?'
    field2 = @db.query_for_value(sql, [1])
    assert_equal 'Two', field2.to_s
    assert_equal true, error_raised
  end

  def test_insert
    sql = 'INSERT INTO table1 ( field1, field2 ) VALUES ( ?, ? );'
    @db.insert(sql, [3, 'Four'])
  end
end
