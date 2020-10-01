import unittest
import weaviate


class TestUserInput(unittest.TestCase):

    def setUp(self) -> None:
        weaviate_url = "http://localhost:8080"
        self.client = weaviate.Client(weaviate_url)
        create_schema.create_schema(schema='project/schema.json', weaviate_url=weaviate_url, overwrite=True)

    def test_papers_only(self):
        pass

    def test_skip_papers(self):
        pass

    def test_skip_authors(self):
        pass

    def test_skip_taxanomy(self):
        pass

    def test_batch(self):
        pass

    def test_batch_size(self):
        pass
