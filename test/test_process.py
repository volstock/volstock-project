import pandas as pd
from unittest.mock import patch
from src.process import (
    get_dim_design,
    get_dim_currency,
    get_currency_names_dataframe,
    get_dim_counterparty,
)


import pandas as pd

def test_get_dim_design():
    sample_data = {
        "design_id": [1, 2, 3],
        "design_name": ["Wooden", "Bronze", "Soft"],
        "file_location": ["/usr", "/private", "/System"],
        "file_name": [
            "wooden-20220717-npgz.json",
            "bronze-20221024-4dds.json",
            "soft-20211001-cjaz.json",
        ],
        "created_at": [
            "2022-11-03 14:20:49.962000",
            "2023-01-12 18:50:09.935000",
            "2023-02-07 17:31:10.093000",
        ],
        "last_updated": [
            "2022-11-03 14:20:49.962000",
            "2022-11-22 15:02:10.226000",
            "2023-02-07 17:31:10.093000",
        ],
    }
    df_design = pd.DataFrame(sample_data)
    result_df = get_dim_design(df_design)
    expected_columns = ["design_name", "file_location", "file_name"]


    assert isinstance(result_df, pd.DataFrame)
    assert "created_at" not in result_df.columns
    assert "last_updated" not in result_df.columns
    assert all(col in result_df.columns for col in expected_columns)
    assert result_df.index.name == "design_id"
    assert len(result_df) == len(df_design)



def test_get_currency_names_dataframe():

    mock_response_data = {
        "usd": "United States Dollar",
        "eur": "Euro",
        "gbp": "British Pound Sterling",
    }

    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = mock_response_data

        result_df = get_currency_names_dataframe()

        expected_columns = ["currency_code", "currency_name"]

        assert isinstance(result_df, pd.DataFrame)
        assert all(col in result_df.columns for col in expected_columns)
        assert len(result_df) == len(mock_response_data)

        expected_data = [
            ("usd", "United States Dollar"),
            ("eur", "Euro"),
            ("gbp", "British Pound Sterling"),
        ]
        for code, name in expected_data:
            assert code in result_df["currency_code"].values
            assert name in result_df["currency_name"].values

        assert all(result_df["currency_code"].str.islower())

        mock_get.assert_called_once_with(
            "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies.json"
        )


def test_get_dim_currency():
    sample_currency_data = {
        "currency_id": [1, 2, 3],
        "currency_code": ["USD", "EUR", "GBP"],
        "created_at": [
            "2022-11-03 14:20:49.962000",
            "2022-11-03 14:20:49.962000",
            "2022-11-03 14:20:49.962000",
        ],
        "last_updated": [
            "2022-11-03 14:20:49.962000",
            "2022-11-03 14:20:49.962000",
            "2022-11-03 14:20:49.962000",
        ],
    }
    df_currency = pd.DataFrame(sample_currency_data)

    mock_currency_names = pd.DataFrame(
        {
            "currency_code": ["usd", "eur", "gbp"],
            "currency_name": ["US Dollar", "Euro", "British Pound"],
        }
    )

    with patch("src.process.get_currency_names_dataframe") as mock_get_names:
        mock_get_names.return_value = mock_currency_names

        result_df = get_dim_currency(df_currency)
        expected_columns = ["currency_code", "currency_name"]
        expected_names = ["US Dollar", "Euro", "British Pound"]

        assert isinstance(result_df, pd.DataFrame)
        assert "created_at" not in result_df.columns
        assert "last_updated" not in result_df.columns
        assert all(col in result_df.columns for col in expected_columns)
        assert result_df.index.name == "currency_id"
        assert result_df["currency_name"].tolist() == expected_names
        assert result_df["currency_code"].tolist() == ["usd", "eur", "gbp"]
        assert list(result_df.index) == [1, 2, 3]

        mock_get_names.assert_called_once()

def test_get_dim_counterparty():
    df_counterparty = pd.DataFrame(
        {
            "counterparty_id": [1, 2, 3],
            "counterparty_legal_name": [
                "Fahey and Sons",
                "Armstrong Inc",
                "Kohler Inc",
            ],
            "legal_address_id": [15, 28, 2],
            "commercial_contact": ["Micheal Toy", "Melba Sanford", "Homer Mitchell"],
            "delivery_contact": ["Myra Kovacek", "Eva Upton", "Veronica Fay"],
        }
    )

    df_address = pd.DataFrame(
        {
            "address_id": [15, 28, 2],
            "address_line_1": ["123 Main St", "456 Maple Ave", "789 Elm St"],
            "address_line_2": ["Suite 100", "Apt 202", "PO Box 303"],
            "district": ["North District", "East District", "West District"],
            "city": ["Springfield", "Shelbyville", "Ogdenville"],
            "postal_code": ["11111", "22222", "33333"],
            "country": ["USA", "USA", "USA"],
            "phone": ["555-1234", "555-5678", "555-9876"],
        }
    )

    result_df = get_dim_counterparty(df_counterparty, df_address)

    expected_columns = [
        "counterparty_legal_name",
        "counterparty_legal_address_line_1",
        "counterparty_legal_address_line_2",
        "counterparty_legal_district",
        "counterparty_legal_city",
        "counterparty_legal_postal_code",
        "counterparty_legal_country",
        "counterparty_legal_phone_number",
    ]


    assert isinstance(result_df, pd.DataFrame)
    assert all(col in result_df.columns for col in expected_columns)
    assert result_df.index.name == "counterparty_id"
    assert result_df.loc[1, "counterparty_legal_name"] == "Fahey and Sons"
    assert result_df.loc[2, "counterparty_legal_address_line_1"] == "456 Maple Ave"
    assert result_df.loc[3, "counterparty_legal_phone_number"] == "555-9876"
    assert list(result_df.index) == [1, 2, 3]