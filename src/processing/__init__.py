"""
Processing / transformation layer public re-exports.

Modules:
  normalizer   — Normalizer (RawJobPosting → JobPosting)
  salary       — SalaryExtractor (text → (min_cad, max_cad, source))
  seniority    — SeniorityInferrer (title → SeniorityLevel)
"""
from src.processing.normalizer import Normalizer
from src.processing.salary import SalaryExtractor
from src.processing.seniority import SeniorityInferrer

__all__ = ["Normalizer", "SalaryExtractor", "SeniorityInferrer"]
