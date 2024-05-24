import os
import unittest
from flask import Flask
from api.app import app

class FlaskTestCase(unittest.TestCase):
    def setUp(self):
        # Sets up the Flask test client
        self.app = app.test_client()
        self.app.testing = True

        # Create a temporary directory for file uploads
        if not os.path.exists('/tmp/uploads'):
            os.makedirs('/tmp/uploads')

        # Create dummy CSV files for testing
        with open('/tmp/uploads/hired_employees.csv', 'w') as f:
            f.write("id,name,datetime,department_id,job_id\n1,John Doe,2021-01-01T00:00:00Z,1,1\n")

        with open('/tmp/uploads/departments.csv', 'w') as f:
            f.write("id,department\n1,IT\n")

        with open('/tmp/uploads/jobs.csv', 'w') as f:
            f.write("id,job\n1,Engineer\n")

    def tearDown(self):
        # Cleanup the temporary files
        if os.path.exists('/tmp/uploads/hired_employees.csv'):
            os.remove('/tmp/uploads/hired_employees.csv')
        if os.path.exists('/tmp/uploads/departments.csv'):
            os.remove('/tmp/uploads/departments.csv')
        if os.path.exists('/tmp/uploads/jobs.csv'):
            os.remove('/tmp/uploads/jobs.csv')

    def test_upload_csv(self):
        # Test the upload CSV endpoint
        with open('/tmp/uploads/hired_employees.csv', 'rb') as f1, \
             open('/tmp/uploads/departments.csv', 'rb') as f2, \
             open('/tmp/uploads/jobs.csv', 'rb') as f3:
            response = self.app.post('/upload-csv', data={
                'files[]': [f1, f2, f3]
            })

        print("Response status code:", response.status_code)
        print("Response data:", response.data.decode('utf-8'))

        self.assertEqual(response.status_code, 200)
        self.assertIn(b'hired_employees.csv', response.data)
        self.assertIn(b'departments.csv', response.data)
        self.assertIn(b'jobs.csv', response.data)

    def test_employees_hired_by_job_department_quarter(self):
        response = self.app.get('/employees-hired-by-job-department-quarter')
        self.assertEqual(response.status_code, 200)

    def test_departments_hired_above_mean(self):
        response = self.app.get('/departments-hired-above-mean')
        self.assertEqual(response.status_code, 200)

if __name__ == '__main__':
    unittest.main()
