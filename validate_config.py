#!/usr/bin/env python
"""Quick validation of configuration for 985-job operation."""

from main import SEARCH_KEYWORDS, PAGE_COUNT, MAX_JOBS
from src.analyzer.turkish_concepts import TurkishConceptAnalyzer

print("=" * 70)
print("KONFİGÜRASYON DOĞRULAMA / CONFIGURATION VALIDATION")
print("=" * 70)

print(f"\n✓ SEARCH_KEYWORDS: {SEARCH_KEYWORDS}")
assert SEARCH_KEYWORDS == ["Yazılım"], "ERROR: Keyword must be ONLY 'Yazılım'"
print("  ✅ PASSED: Single Turkish keyword 'Yazılım'")

print(f"\n✓ PAGE_COUNT: {PAGE_COUNT}")
assert PAGE_COUNT == 50, "ERROR: PAGE_COUNT must be 50"
print("  ✅ PASSED: 50 pages configured")

print(f"\n✓ MAX_JOBS: {MAX_JOBS}")
assert MAX_JOBS == 1000, "ERROR: MAX_JOBS must be 1000"
print("  ✅ PASSED: 1000 jobs maximum configured")

print("\n" + "=" * 70)
print("TÜRKÇE KAVRAMLAR / TURKISH CONCEPTS")
print("=" * 70)

analyzer = TurkishConceptAnalyzer()

print(f"\n✓ Dijital Konseptler / Digital Concepts: {len(analyzer.DIGITAL_CONCEPTS)}")
assert len(analyzer.DIGITAL_CONCEPTS) == 5, "ERROR: Must have EXACTLY 5 digital concepts"
for concept in analyzer.DIGITAL_CONCEPTS.keys():
    print(f"  • {concept}")

print(f"\n✓ Yeşil Konseptler / Green Concepts: {len(analyzer.GREEN_CONCEPTS)}")
assert len(analyzer.GREEN_CONCEPTS) == 5, "ERROR: Must have EXACTLY 5 green concepts"
for concept in analyzer.GREEN_CONCEPTS.keys():
    print(f"  • {concept}")

print("\n" + "=" * 70)
print("TEST: KAVRAM ÇIKARMA / CONCEPT EXTRACTION")
print("=" * 70)

test_text = "Bu şirket dijital dönüşüm yapıyor. Yeşil dönüşüm stratejimiz sürdürülebilirlik ve yenilenebilir enerji kullanımına odaklanıyor. Yazılım mimarı aranıyor."
result = analyzer.analyze(test_text)

print(f"\nTest Text: {test_text[:80]}...")
print(f"Dijital Kavramlar Bulundu: {result['digital']}")
print(f"Yeşil Kavramlar Bulundu: {result['green']}")

assert len(result['digital']) > 0, "ERROR: Should detect digital concepts"
assert len(result['green']) > 0, "ERROR: Should detect green concepts"
print("\n✅ PASSED: Concept extraction working correctly")

print("\n" + "=" * 70)
print("ALL VALIDATION TESTS PASSED ✅")
print("=" * 70)
print("\nReady for 985-job operation!")
print("Command: python main.py --run")
