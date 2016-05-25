import os
from unittest import TestCase

from nose.twistedtools import deferred

from StormDBManager import StormDBManager


class TestStormDBManager(TestCase):

    FILE_DIR = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
    TEST_DATA_DIR = os.path.abspath(os.path.join(FILE_DIR, u"data"))
    SQLITE_TEST_DB = os.path.abspath(os.path.join(TEST_DATA_DIR, u"test.db"))

    def setUp(self):
        super(TestStormDBManager, self).setUp()
        self.storm_db = StormDBManager("sqlite:%s" % self.SQLITE_TEST_DB)

    def tearDown(self):
        super(TestStormDBManager, self).tearDown()
        # Delete the database file.
        os.unlink(self.SQLITE_TEST_DB)

    def create_car_database(self):
        """
        Creates a database with the name "car".
        Contains one column named "brand".
        :return: A deferred that fires once the table has been made.
        """
        sql = u"CREATE TABLE car(brand);"
        return self.storm_db.schedule_query(self.storm_db.execute_query, sql)


    @deferred(timeout=5)
    def test_execute_function(self):
        def check_return_is_none(result):
            self.assertIsNone(result)

        result_deferred = self.create_car_database()
        result_deferred.addCallback(check_return_is_none)
        return result_deferred

    @deferred(timeout=5)
    def test_insert_and_fetchone(self):
        """
        This test tests the insert functionality and the fetch_one function.
        """
        def assert_result(result):
            self.assertIsInstance(result, tuple, "Result was not a tuple!")
            self.assertEquals(result[0], "BMW", "Result did not contain BMW as expected!")

        def fetch_inserted(_):
            sql = u"SELECT * FROM car"
            return self.storm_db.schedule_query(self.storm_db.fetch_one, sql)

        def insert_into_db(_):
            return self.storm_db.schedule_query(self.storm_db.insert, "car", brand="BMW")

        result_deferred = self.create_car_database() # Create the database
        result_deferred.addCallback(insert_into_db) # Insert one value
        result_deferred.addCallback(fetch_inserted) # Fetch the value
        result_deferred.addCallback(assert_result) # Assert the result

        return result_deferred

    @deferred(timeout=5)
    def test_insert_and_fetchall(self):
        """
        This test tests the insert_many functionality and the fetch_all functionality.
        """
        def assert_result(result):
            self.assertIsInstance(result, list, "Result was not a list!")
            self.assertEquals(result[0][0], "BMW", "First result did not contain BMW as expected!")
            self.assertEquals(result[1][0], "Volvo", "Seconds result did not contain Volvo as expected!")

        def fetch_inserted(_):
            sql = u"SELECT * FROM car"
            return self.storm_db.schedule_query(self.storm_db.fetch_all, sql)

        def insert_into_db(_):
            list = []
            list.append({"brand" : "BMW"})
            list.append({"brand" : "Volvo"})
            return self.storm_db.schedule_query(self.storm_db.insert_many, "car", list)

        result_deferred = self.create_car_database() # Create the database
        result_deferred.addCallback(insert_into_db) # Insert two value
        result_deferred.addCallback(fetch_inserted) # Fetch all values
        result_deferred.addCallback(assert_result) # Assert the results

        return result_deferred

    @deferred(timeout=5)
    def test_remove_single_element(self):
        """
        This test tests the delete function by using a single element as value.
        """
        def assert_result(result):
            self.assertIsInstance(result, list, "Result was not a list!")
            self.assertEquals(result[0][0], "Volvo", "First result was not Volvo as expected!")

        def fetch_inserted(_):
            sql = u"SELECT * FROM car"
            return self.storm_db.schedule_query(self.storm_db.fetch_all, sql)

        def delete_one(_):
            return self.storm_db.schedule_query(self.storm_db.delete, "car", brand="BMW")

        def insert_into_db(_):
            list = []
            list.append({"brand" : "BMW"})
            list.append({"brand" : "Volvo"})
            return self.storm_db.schedule_query(self.storm_db.insert_many, "car", list)

        result_deferred = self.create_car_database() # Create the database
        result_deferred.addCallback(insert_into_db) # Insert two value
        result_deferred.addCallback(delete_one) # Delete one value by using a single element
        result_deferred.addCallback(fetch_inserted) # Fetch all values
        result_deferred.addCallback(assert_result) # Assert the results

        return result_deferred

    @deferred(timeout=5)
    def test_remove_tuple(self):
        """
        This test tests the delete function by using a tuple as value.
        """
        def assert_result(result):
            self.assertIsInstance(result, list, "Result was not a list!")
            self.assertEquals(result[0][0], "Volvo", "First result was not Volvo as expected!")

        def fetch_inserted(_):
            sql = u"SELECT * FROM car"
            return self.storm_db.schedule_query(self.storm_db.fetch_all, sql)

        def delete_one(_):
            return self.storm_db.schedule_query(self.storm_db.delete, "car", brand=("LIKE", "BMW"))

        def insert_into_db(_):
            list = []
            list.append({"brand" : "BMW"})
            list.append({"brand" : "Volvo"})
            return self.storm_db.schedule_query(self.storm_db.insert_many, "car", list)

        result_deferred = self.create_car_database() # Create the database
        result_deferred.addCallback(insert_into_db) # Insert two value
        result_deferred.addCallback(delete_one) # Delete one value by using a tuple containing an operator
        result_deferred.addCallback(fetch_inserted) # Fetch all values
        result_deferred.addCallback(assert_result) # Assert the results

        return result_deferred

    @deferred(timeout=5)
    def test_size(self):
        """
        This test tests the size function.
        """
        def assert_result(result):
            self.assertIsInstance(result, tuple, "Result was not a tuple!")
            self.assertEquals(result[0], 2, "Result was not 2")

        def get_size(_):
            return self.storm_db.schedule_query(self.storm_db.num_rows, "car")

        def insert_into_db(_):
            list = []
            list.append({"brand" : "BMW"})
            list.append({"brand" : "Volvo"})
            return self.storm_db.schedule_query(self.storm_db.insert_many, "car", list)

        result_deferred = self.create_car_database() # Create the database
        result_deferred.addCallback(insert_into_db) # Insert two value
        result_deferred.addCallback(get_size) # Get the size
        result_deferred.addCallback(assert_result) # Assert the result

        return result_deferred
