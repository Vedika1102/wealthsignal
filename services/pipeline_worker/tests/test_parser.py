import unittest

from wealthsignal_pipeline.edgar_client import filing_index_url, normalize_cik, submissions_url
from wealthsignal_pipeline.parser import parse_information_table


SAMPLE_INFORMATION_TABLE = """<?xml version="1.0" encoding="UTF-8"?>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
  <infoTable>
    <nameOfIssuer>APPLE INC</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>037833100</cusip>
    <value>125000</value>
    <shrsOrPrnAmt>
      <sshPrnamt>500000</sshPrnamt>
      <sshPrnamtType>SH</sshPrnamtType>
    </shrsOrPrnAmt>
    <investmentDiscretion>SOLE</investmentDiscretion>
    <otherManager></otherManager>
    <votingAuthority>
      <Sole>500000</Sole>
      <Shared>0</Shared>
      <None>0</None>
    </votingAuthority>
  </infoTable>
  <infoTable>
    <nameOfIssuer>NVIDIA CORP</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>67066G104</cusip>
    <value>98000</value>
    <shrsOrPrnAmt>
      <sshPrnamt>250000</sshPrnamt>
      <sshPrnamtType>SH</sshPrnamtType>
    </shrsOrPrnAmt>
    <putCall>CALL</putCall>
    <investmentDiscretion>SHARED</investmentDiscretion>
    <votingAuthority>
      <Sole>0</Sole>
      <Shared>250000</Shared>
      <None>0</None>
    </votingAuthority>
  </infoTable>
</informationTable>
"""


class ParseInformationTableTests(unittest.TestCase):
    def test_parse_information_table_extracts_holdings(self) -> None:
        parsed = parse_information_table(
            SAMPLE_INFORMATION_TABLE,
            cik="0001067983",
            accession_number="0001067983-24-000001",
            filer_name="Example Capital Management",
        )

        self.assertEqual(parsed.filing.cik, "0001067983")
        self.assertEqual(parsed.filing.accession_number, "0001067983-24-000001")
        self.assertEqual(len(parsed.holdings), 2)

        apple = parsed.holdings[0]
        self.assertEqual(apple.issuer_name, "APPLE INC")
        self.assertEqual(apple.cusip, "037833100")
        self.assertEqual(apple.value_thousands, 125000)
        self.assertEqual(apple.market_value_usd, 125000000)
        self.assertEqual(apple.shares_or_principal, 500000)
        self.assertEqual(apple.put_call, "")

    def test_parse_information_table_handles_optional_tags(self) -> None:
        parsed = parse_information_table(
            SAMPLE_INFORMATION_TABLE,
            cik="0001067983",
            accession_number="0001067983-24-000001",
        )

        nvidia = parsed.holdings[1]
        self.assertEqual(nvidia.issuer_name, "NVIDIA CORP")
        self.assertEqual(nvidia.put_call, "CALL")
        self.assertEqual(nvidia.investment_discretion, "SHARED")
        self.assertEqual(nvidia.voting_authority_shared, 250000)


class EdgarClientUtilityTests(unittest.TestCase):
    def test_normalize_cik_zero_pads_to_ten_digits(self) -> None:
        self.assertEqual(normalize_cik("1067983"), "0001067983")

    def test_submissions_url_uses_zero_padded_cik(self) -> None:
        self.assertEqual(
            submissions_url("1067983"),
            "https://data.sec.gov/submissions/CIK0001067983.json",
        )

    def test_filing_index_url_removes_accession_hyphens(self) -> None:
        self.assertEqual(
            filing_index_url("1067983", "0001067983-24-000001"),
            "https://www.sec.gov/Archives/edgar/data/1067983/000106798324000001",
        )


if __name__ == "__main__":
    unittest.main()
