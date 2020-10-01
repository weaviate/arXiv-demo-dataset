import unittest
import weaviate
from project.create_schema import create_schema


class TestCreateSchema(unittest.TestCase):

    def setUp(self) -> None:
        create_schema(schema='project/schema.json', overwrite=True)
        self.client = weaviate.Client("http://localhost:8080")

    def test_schema_created(self):
        self.assertTrue(self.client.schema.contains(), "Container should already contain schema")

        schema = self.client.schema.get()
        self.assertIsNotNone(schema)

        class_list = schema.get("things").get("classes")
        self.assertIsNotNone(class_list)
        self.assertEqual({'Group', 'Archive', 'Category', 'Journal', 'Author', 'Paper'}, set([e.get("class") for e in class_list]))

    def test_not_overwrite_schema(self):
        create_schema(schema='test/test_overwrite_schema.json', overwrite=False)
        self.assertTrue(self.client.schema.contains(), "Container should contain a schema")

        schema = self.client.schema.get()
        self.assertIsNotNone(schema)

        class_list = schema.get("things").get("classes")
        self.assertIsNotNone(class_list)
        self.assertEqual({'Group', 'Archive', 'Category', 'Journal', 'Author', 'Paper'}, set([e.get("class") for e in class_list]))

    def test_overwrite_schema(self):
        create_schema(schema='test/test_overwrite_schema.json', overwrite=True)
        self.assertTrue(self.client.schema.contains(), "Container should contain a schema")

        schema = self.client.schema.get()
        self.assertIsNotNone(schema)

        class_list = schema.get("things").get("classes")
        self.assertIsNotNone(class_list)
        self.assertEqual({'Group', 'Archive', 'Category', 'Journal', 'Author', 'Paper',
                          'NewClass'}, set([e.get("class") for e in class_list]))
