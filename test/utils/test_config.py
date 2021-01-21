import unittest
from src.truefilm.utils.config import Config

class TestConfig(unittest.TestCase):

    def test_resources_final_ko(self):

        config = Config(folders_final=['../test_resources/final_ko', 'test/test_resources/final_ko'],
                        folders_raw=['../test_resources/raw_ko', 'test/test_resources/raw_ko'])

        expected_paths = {
            'movies': None,
            'enwiki': None
        }

        output_paths = config.final_resources_check(file_names=['movies','enwiki'])

        self.assertDictEqual(expected_paths, output_paths)

    def test_resources_final_ok(self):

        config = Config(folders_final=['../test_resources/final_ok','test/test_resources/final_ok'],
                        folders_raw=['../test_resources/raw_ok','test/test_resources/raw_ok'])

        output_paths = config.final_resources_check(file_names=['movies','enwiki'])

        expected_paths = {
            'movies': output_paths['movies'],
            'enwiki': output_paths['enwiki']
        }


        self.assertDictEqual(expected_paths, output_paths)

    def test_resources_raw_ok(self):

        config = Config(folders_final=['../test_resources/final_ok','test/test_resources/final_ok'],
                        folders_raw=['../test_resources/raw_ok','test/test_resources/raw_ok'])

        output_paths = config.raw_resources_check(file_names=['movies.csv','enwiki-test.xml'])

        expected_paths = {
            'movies.csv': output_paths['movies.csv'],
            'enwiki-test.xml': output_paths['enwiki-test.xml']
        }


        self.assertDictEqual(expected_paths, output_paths)

    def test_resources_raw_ko(self):

        config = Config(folders_final=['../test_resources/final_ko','test/test_resources/final_ko'],
                        folders_raw=['../test_resources/raw_ko','test/test_resources/raw_ko'])

        output_paths = config.raw_resources_check(file_names=['movies.csv','enwiki-test.xml'])

        expected_paths = {
            'movies.csv': None,
            'enwiki-test.xml': None
        }


        self.assertDictEqual(expected_paths, output_paths)

    def test_resources_raw_path_movies_typo(self):

        config = Config(folders_final=['../test_resources/final_ok','test/test_resources/final_ok'],
                        folders_raw=['../test_resources/raw_ok','test/test_resources/raw_ok'])

        output_paths = config.raw_resources_check(file_names=['moviesss.csv','enwiki-test.xml'])

        expected_paths = {
            'moviesss.csv': None,
            'enwiki-test.xml': output_paths['enwiki-test.xml']
        }


        self.assertDictEqual(expected_paths, output_paths)

    def test_resources_raw_path_wiki_typo(self):

        config = Config(folders_final=['../test_resources/final_ok','test/test_resources/final_ok'],
                        folders_raw=['../test_resources/raw_ok','test/test_resources/raw_ok'])

        output_paths = config.raw_resources_check(file_names=['movies.csv','enwiki-testtt.xml'])

        expected_paths = {
            'movies.csv': output_paths['movies.csv'],
            'enwiki-testtt.xml': None
        }


        self.assertDictEqual(expected_paths, output_paths)
