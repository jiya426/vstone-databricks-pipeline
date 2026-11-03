"""Unit tests for column descriptions generated from DataDictionary.csv."""
from vstone_framework.metadata import column_comment, column_comments, load_data_dictionary

TERMS = ["AgeOfInventory", "DaysOnZillow", "InventorySeasonallyAdjusted", "InventoryRaw", "MedianListingPricePerSqft",
         "MedianListingPrice", "MedianPctOfPriceReduction", "MedianPriceCutDollar", "MedianRentalPricePerSqft",
         "MedianRentalPrice", "MedianSoldPrice", "PctOfHomesDecreasingInValues", "PctOfHomesIncreasingInValues",
         "PctOfHomesSellingForGain", "PctOfHomesSellingForLoss", "PctOfListingsWithPriceReductionsSeasAdj",
         "PctOfListingsWithPriceReductions", "PriceToRentRatio", "SalesSeasonallyAdjusted", "SalesRaw", "TierShare",
         "ZHVI", "ZHVIPerSqft", "ZRI", "ZriPerSqft"]
DICT = {t: f"Definition of {t}." for t in TERMS}

# Every metric column name found in the dataset (all_available_metrics.json, minus Date/RegionName)
REAL_COLUMNS = ["AgeOfInventory", "DaysOnZillow_AllHomes", "InventoryRaw_AllHomes", "InventorySeasonallyAdjusted_AllHomes",
    "InventorySeasonallyAdjusted_BottomTier", "InventoryTierShare_BottomTier_AllHomes", "MedianListingPricePerSqft_1Bedroom",
    "MedianListingPrice_SingleFamilyResidence", "MedianPctOfPriceReduction_CondoCoop", "MedianPriceCutDollar_AllHomes",
    "MedianRentalPricePerSqft_Studio", "MedianRentalPrice_MultiFamilyResidence5PlusUnits",
    "PctOfHomesDecreasingInValues_AllHomes", "PctOfHomesSellingForLoss_AllHomes",
    "PctOfListingsWithPriceReductionsSeasAdj_TopTier", "PctOfListingsWithPriceReductions_CondoCoop",
    "PriceToRentRatio_AllHomes", "Sale_Counts", "Sale_Counts_Msa", "Sale_Counts_Seas_Adj", "Sale_Counts_Seas_Adj_Msa",
    "Sale_Prices", "Sale_Prices_Msa", "ZHVIPerSqft_AllHomes", "ZHVI_1bedroom", "ZHVI_TopTier", "ZRI_AllHomes",
    "ZRI_AllHomesPlusMultifamily", "ZriPerSqft_AllHomes", "Zri_MultiFamilyResidenceRental",
    "Zri_SingleFamilyResidenceRental"]


def test_every_real_metric_gets_a_dictionary_definition():
    for col in REAL_COLUMNS:
        assert column_comment(col, DICT).startswith("Definition of"), col


def test_longest_family_wins_and_segment_is_kept():
    assert column_comment("MedianListingPricePerSqft_3Bedroom", DICT) == \
        "Definition of MedianListingPricePerSqft. Segment: 3Bedroom."
    assert column_comment("PctOfListingsWithPriceReductionsSeasAdj_TopTier", DICT).startswith(
        "Definition of PctOfListingsWithPriceReductionsSeasAdj.")
    assert column_comment("Sale_Counts_Seas_Adj", DICT) == "Definition of SalesSeasonallyAdjusted."


def test_fixed_and_unknown_columns():
    assert column_comment("FIPS", DICT).startswith("5-digit county FIPS")
    assert column_comment("Mystery_Col", DICT) == "Zillow metric Mystery_Col"
    assert set(column_comments(["Date", "ZHVI_AllHomes"], DICT)) == {"Date", "ZHVI_AllHomes"}


def test_load_data_dictionary_reads_windows_1252(tmp_path):
    p = tmp_path / "DataDictionary.csv"
    p.write_bytes('Variable,Definition\nZHVIPerSqft,"Value by the home\x92s   square footage"\n'.encode("latin-1"))
    assert load_data_dictionary(str(p)) == {"ZHVIPerSqft": "Value by the home’s square footage"}
