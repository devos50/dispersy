import os
import unittest

from twisted.internet.defer import DeferredList
from unittest import TestCase

from nose.twistedtools import deferred

from ..StormDBManager import StormDBManager


class TestStormDBManager(TestCase):
    FILE_DIR = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
    TEST_DATA_DIR = os.path.abspath(os.path.join(FILE_DIR, u"data"))
    SQLITE_TEST_DB = os.path.abspath(os.path.join(TEST_DATA_DIR, u"test.db"))

    def setUp(self):
        super(TestStormDBManager, self).setUp()

        # Do not use an in-memory database. Different connections to the same
        # in-memory database do not point towards the same database.
        # http://stackoverflow.com/questions/3315046/sharing-a-memory-database-between-different-threads-in-python-using-sqlite3-pa
        self.storm_db = StormDBManager("sqlite:%s" % self.SQLITE_TEST_DB)
        self.storm_db.initialize()

    def tearDown(self):
        super(TestStormDBManager, self).tearDown()
        # Delete the database file if not using an in-memory database.
        if os.path.exists(self.SQLITE_TEST_DB):
            os.unlink(self.SQLITE_TEST_DB)

    def create_car_database(self):
        """
        Creates a table with the name "car".
        Contains one column named "brand".
        :return: A deferred that fires once the table has been made.
        """
        sql = u"CREATE TABLE car(brand);"
        return self.storm_db.execute(sql)

    def create_myinfo_table(self):
        """
        Creates a table with the name "MyInfo".
        Contains two columns: one with "entry" and one named "value".
        :return: A deferred that fires once the table has been made.
        """
        sql = u"""
            CREATE TABLE MyInfo (
            entry  PRIMARY KEY,
            value  text
            );
        """
        return self.storm_db.execute(sql)

    @unittest.skip("Re-enable when he storm db manager uses the deferred lock again.")
    @deferred(timeout=5)
    def test_execute_function(self):
        def check_return_is_none(result):
            self.assertIsNone(result)

        result_deferred = self.create_car_database()
        result_deferred.addCallback(check_return_is_none)
        return result_deferred

    @unittest.skip("Re-enable when he storm db manager uses the deferred lock again.")
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
            return self.storm_db.fetchone(sql)

        def insert_into_db(_):
            return self.storm_db.insert( "car", brand="BMW")

        result_deferred = self.create_car_database()  # Create the car table
        result_deferred.addCallback(insert_into_db)  # Insert one value
        result_deferred.addCallback(fetch_inserted)  # Fetch the value
        result_deferred.addCallback(assert_result)  # Assert the result

        return result_deferred

    @unittest.skip("Re-enable when he storm db manager uses the deferred lock again.")
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
            return self.storm_db.fetchall(sql)

        def insert_into_db(_):
            list = []
            list.append({"brand": "BMW"})
            list.append({"brand": "Volvo"})
            return self.storm_db.insert_many( "car", list)

        result_deferred = self.create_car_database()  # Create the car table
        result_deferred.addCallback(insert_into_db)  # Insert two value
        result_deferred.addCallback(fetch_inserted)  # Fetch all values
        result_deferred.addCallback(assert_result)  # Assert the results

        return result_deferred

    @unittest.skip("Re-enable when he storm db manager uses the deferred lock again.")
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
            return self.storm_db.fetchall(sql)

        def delete_one(_):
            return self.storm_db.delete( "car", brand="BMW")

        def insert_into_db(_):
            list = []
            list.append({"brand": "BMW"})
            list.append({"brand": "Volvo"})
            return self.storm_db.insert_many("car", list)

        result_deferred = self.create_car_database()  # Create the car table
        result_deferred.addCallback(insert_into_db)  # Insert two value
        result_deferred.addCallback(delete_one)  # Delete one value by using a single element
        result_deferred.addCallback(fetch_inserted)  # Fetch all values
        result_deferred.addCallback(assert_result)  # Assert the results

        return result_deferred

    @unittest.skip("Re-enable when he storm db manager uses the deferred lock again.")
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
            return self.storm_db.fetchall(sql)

        def delete_one(_):
            return self.storm_db.delete("car", brand=("LIKE", "BMW"))

        def insert_into_db(_):
            list = []
            list.append({"brand": "BMW"})
            list.append({"brand": "Volvo"})
            return self.storm_db.insert_many("car", list)

        result_deferred = self.create_car_database()  # Create the car table
        result_deferred.addCallback(insert_into_db)  # Insert two value
        result_deferred.addCallback(delete_one)  # Delete one value by using a tuple containing an operator
        result_deferred.addCallback(fetch_inserted)  # Fetch all values
        result_deferred.addCallback(assert_result)  # Assert the results

        return result_deferred

    @unittest.skip("Re-enable when he storm db manager uses the deferred lock again.")
    @deferred(timeout=5)
    def test_size(self):
        """
        This test tests the size function.
        """

        def assert_result(result):
            self.assertIsInstance(result, tuple, "Result was not a tuple!")
            self.assertEquals(result[0], 2, "Result was not 2")

        def get_size(_):
            return self.storm_db.count("car")

        def insert_into_db(_):
            list = []
            list.append({"brand": "BMW"})
            list.append({"brand": "Volvo"})
            return self.storm_db.insert_many("car", list)

        result_deferred = self.create_car_database()  # Create the car table
        result_deferred.addCallback(insert_into_db)  # Insert two value
        result_deferred.addCallback(get_size)  # Get the size
        result_deferred.addCallback(assert_result)  # Assert the result

        return result_deferred

    @unittest.skip("Re-enable when he storm db manager uses the deferred lock again.")
    @deferred(timeout=5)
    def test_version_no_table(self):
        """
        This test tests whether the version is 0 if an sql error occurs.
        In this case the table MyInfo does not exist.
        """

        def assert_result(_):
            self.assertIsInstance(self.storm_db._version, int, "_version field is not an int!")
            self.assertEqual(self.storm_db._version, 0, "Version was not 0 but: %r" % self.storm_db._version)

        def get_size(_):
            return self.storm_db.count("car")

        result_deferred = self.create_car_database()  # Create the car table
        result_deferred.addCallback(get_size)  # Get the version
        result_deferred.addCallback(assert_result)  # Assert the version

        return result_deferred

    @unittest.skip("Re-enable when he storm db manager uses the deferred lock again.")
    @deferred(timeout=5)
    def test_version_myinfo_table(self):
        """
        This test tests whether the version is 2 if the MyInfo table exists.
        """

        def assert_result(_):
            self.assertIsInstance(self.storm_db._version, int, "_version field is not an int!")
            self.assertEqual(self.storm_db._version, 2, "Version was not 2 but: %r" % self.storm_db._version)

        def get_version(_):
            return self.storm_db._retrieve_version()

        def insert_version(_):
            return self.storm_db.insert("MyInfo", entry="version", value="2")

        result_deferred = self.create_myinfo_table()  # Create the database
        result_deferred.addCallback(insert_version)  # Get the version
        result_deferred.addCallback(get_version)  # Let the manager retrieve the version (again).
        result_deferred.addCallback(assert_result)  # Assert the version

        return result_deferred

    @unittest.skip("Re-enable when he storm db manager uses the deferred lock again.")
    @deferred(timeout=5)
    def test_synchronous_insert_with_lock(self):
        """
        This test tests that if you schedule three calls simultaneously, that
        by the mechanism of the lock they still are executed synchronously.
        """

        def assert_sequence(result):
            self.assertIsInstance(result, list, "Result was not of type list but: %r" % result)
            self.assertEqual(len(result), 3, "Result list didn't contain 3 tuples but: %r" % len(result))
            self.assertEqual(result[0][1], 1)
            self.assertEqual(result[1][1], 2)
            self.assertEqual(result[2][1], 3)

        def fetch_all(_):
            sql = u"SELECT * FROM numtest"
            return self.storm_db.fetchall(sql)

        defer_list = []

        def schedule_tree_inserts(_):
            for i in xrange(1, 4):
                defer_list.append(self.storm_db.insert( "numtest", num=i))

            return DeferredList(defer_list)

        def create_numtest_db():
            sql = u"""
                CREATE TABLE numtest (
                  id INTEGER PRIMARY KEY,
                  num INTEGER
                );
            """
            return self.storm_db.execute(sql)

        result_deferred = create_numtest_db()
        result_deferred.addCallback(schedule_tree_inserts)
        result_deferred.addCallback(fetch_all)
        result_deferred.addCallback(assert_sequence)

        return result_deferred

    # TODO add test which covers getlastrowid being true.
