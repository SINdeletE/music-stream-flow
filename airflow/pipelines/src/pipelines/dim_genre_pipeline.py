from src.extractors import GenreExtractor
from src.loaders import DimGenreLoader
from src.models.dwh import DimGenre
from src.models.oltp import Genre
from src.transformers import DimGenreTransformer

from ._pipeline import BasePipeline


class DimGenrePipeline(BasePipeline[Genre, DimGenre]):
    extractor = GenreExtractor
    transformer = DimGenreTransformer
    loader = DimGenreLoader
