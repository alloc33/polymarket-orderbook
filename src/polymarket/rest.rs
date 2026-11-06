//! Lists Polymarket's live assets through the Gamma API.

use std::time::Duration;

use serde::Deserialize;
use tokio::sync::mpsc;
use tracing::warn;

// Events nest their markets, so one page carries ~900 markets; the markets endpoint
// gives 100 per page and caps offset pagination at 2000.
const EVENTS_URL: &str = "https://gamma-api.polymarket.com/events/keyset";
const PAGE_LIMIT: &str = "100";
const ATTEMPTS: u32 = 5;
const RETRY_DELAY: Duration = Duration::from_secs(1);

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
    active: bool,
    #[serde(default)]
    accepting_orders: bool,
    #[serde(default)]
    enable_order_book: bool,
    // A JSON array encoded as a string.
    clob_token_ids: Option<String>,
}

impl Market {
    // Inactive markets are placeholders (for example every possible nominee of a future
    // election); they never trade and would only occupy sockets.
    fn assets(&self) -> Vec<String> {
        if !(self.active && self.accepting_orders && self.enable_order_book) {
            return Vec::new();
        }
        let Some(ids) = &self.clob_token_ids else {
            return Vec::new();
        };
        serde_json::from_str(ids)
            .inspect_err(|error| warn!(%error, "ignored market with an unreadable asset list"))
            .unwrap_or_default()
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
            for asset in market.assets() {
                // The receiver stopped listening (--limit); nothing more to fetch.
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
                tokio::time::sleep(RETRY_DELAY * attempt).await;
                attempt += 1;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const YES: &str =
        "21910945789202551310737182019481519202352824012676720876277298283641159588352";
    const NO: &str =
        "90227223115596293448966158151606153337847900483048338623540402385929720677904";

    fn market(active: bool, accepting_orders: bool, ids: Option<&str>) -> Market {
        Market {
            active,
            accepting_orders,
            enable_order_book: true,
            clob_token_ids: ids.map(str::to_owned),
        }
    }

    #[test]
    fn page_yields_assets_of_live_markets_only() {
        let json = format!(
            r#"{{
                "events": [
                    {{
                        "id": "12345",
                        "title": "Who will win?",
                        "markets": [
                            {{"id": "1", "active": true, "acceptingOrders": true, "enableOrderBook": true,
                             "clobTokenIds": "[\"{YES}\", \"{NO}\"]"}},
                            {{"id": "2", "active": false, "acceptingOrders": true, "enableOrderBook": true,
                             "clobTokenIds": "[\"1\", \"2\"]"}},
                            {{"id": "3", "active": true, "acceptingOrders": false, "enableOrderBook": true,
                             "clobTokenIds": "[\"3\", \"4\"]"}},
                            {{"id": "4", "active": true, "acceptingOrders": true, "enableOrderBook": true,
                             "clobTokenIds": null}}
                        ]
                    }},
                    {{ "id": "12346", "title": "Event without markets" }}
                ],
                "next_cursor": "ydSsD4qHilYotO3MIYTL75t4"
            }}"#
        );

        let page: EventsPage = serde_json::from_str(&json).unwrap();
        let assets: Vec<String> = page
            .events
            .iter()
            .flat_map(|event| &event.markets)
            .flat_map(Market::assets)
            .collect();

        assert_eq!(assets, [YES, NO]);
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
    fn unreadable_asset_list_skips_the_market() {
        assert_eq!(
            market(true, true, Some("not json")).assets(),
            Vec::<String>::new()
        );
    }
}
