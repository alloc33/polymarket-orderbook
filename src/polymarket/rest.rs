use serde::Deserialize;
use tokio::sync::mpsc;
use tracing::warn;

const EVENTS_URL: &str = "https://gamma-api.polymarket.com/events/keyset";
const PAGE_LIMIT: &str = "100";
const ATTEMPTS: u32 = 3;

#[derive(Deserialize)]
struct EventsPage {
    events: Vec<Event>,
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
struct Event {
    #[serde(default)]
    markets: Vec<Market>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Market {
    #[serde(default)]
    accepting_orders: bool,
    #[serde(default)]
    enable_order_book: bool,
    clob_token_ids: Option<String>,
}

impl Market {
    fn assets(&self) -> anyhow::Result<Vec<String>> {
        if !(self.accepting_orders && self.enable_order_book) {
            return Ok(Vec::new());
        }
        match &self.clob_token_ids {
            Some(ids) => Ok(serde_json::from_str(ids)?),
            None => Ok(Vec::new()),
        }
    }
}

pub async fn live_assets(
    client: &reqwest::Client,
    assets: &mpsc::UnboundedSender<String>,
) -> anyhow::Result<()> {
    let mut cursor = None;

    loop {
        let page = fetch_page(client, cursor.as_deref()).await?;

        for market in page.events.iter().flat_map(|event| &event.markets) {
            for asset in market.assets()? {
                if assets.send(asset).is_err() {
                    return Ok(());
                }
            }
        }

        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok(()),
        }
    }
}

async fn fetch_page(client: &reqwest::Client, cursor: Option<&str>) -> anyhow::Result<EventsPage> {
    let mut attempt = 1;

    loop {
        let mut request = client.get(EVENTS_URL).query(&[
            ("active", "true"),
            ("closed", "false"),
            ("limit", PAGE_LIMIT),
        ]);
        if let Some(cursor) = cursor {
            request = request.query(&[("after_cursor", cursor)]);
        }

        let result = async { request.send().await?.error_for_status()?.json().await }.await;

        match result {
            Ok(page) => return Ok(page),
            Err(error) if attempt < ATTEMPTS => {
                warn!(%error, attempt, "retrying events page");
                attempt += 1;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_yields_assets_of_tradable_markets_only() {
        let json = r#"{
            "events": [
                {
                    "id": "12345",
                    "title": "Xi Jinping out before 2027?",
                    "markets": [
                        {
                            "id": "559651",
                            "acceptingOrders": true,
                            "enableOrderBook": true,
                            "clobTokenIds": "[\"32338220190071351435\", \"25659310674993675562\"]"
                        },
                        {
                            "id": "559652",
                            "acceptingOrders": false,
                            "enableOrderBook": true,
                            "clobTokenIds": "[\"11111111111111111111\", \"22222222222222222222\"]"
                        },
                        {
                            "id": "559653",
                            "acceptingOrders": true,
                            "enableOrderBook": true,
                            "clobTokenIds": null
                        }
                    ]
                },
                { "id": "12346", "title": "Event without markets" }
            ],
            "next_cursor": "ydSsD4qHilYotO3MIYTL75t4"
        }"#;

        let page: EventsPage = serde_json::from_str(json).unwrap();
        let assets: Vec<String> = page
            .events
            .iter()
            .flat_map(|event| &event.markets)
            .map(Market::assets)
            .collect::<anyhow::Result<Vec<_>>>()
            .unwrap()
            .concat();

        assert_eq!(assets, ["32338220190071351435", "25659310674993675562"]);
        assert_eq!(
            page.next_cursor.as_deref(),
            Some("ydSsD4qHilYotO3MIYTL75t4")
        );
    }

    #[test]
    fn last_page_has_no_cursor() {
        let page: EventsPage = serde_json::from_str(r#"{"events": []}"#).unwrap();

        assert!(page.events.is_empty());
        assert!(page.next_cursor.is_none());
    }

    #[test]
    fn malformed_asset_list_is_an_error() {
        let market = Market {
            accepting_orders: true,
            enable_order_book: true,
            clob_token_ids: Some("not json".to_owned()),
        };

        assert!(market.assets().is_err());
    }
}
