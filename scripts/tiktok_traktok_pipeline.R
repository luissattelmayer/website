#!/usr/bin/env Rscript
# ---------------------------------------------------------------------------
# TikTok Research API pipeline using {traktok} (https://github.com/JBGruber/traktok)
#
# Pulls every available video for a list of TikTok accounts ("parties") via
# the official TikTok Research API and combines the results into one data
# frame, using {purrr} for the iteration.
#
# Prerequisites:
#   - Approved TikTok Research API access (client_key / client_secret) from
#     https://developers.tiktok.com/products/research-api/
#   - install.packages("traktok")   # CRAN
#     # or the dev version:
#     # remotes::install_github("JBGruber/traktok")
#   - install.packages(c("purrr", "dplyr", "readr"))
# ---------------------------------------------------------------------------

library(traktok)
library(purrr)
library(dplyr)
library(readr)

# --- 1. Authenticate --------------------------------------------------------
# Don't hardcode credentials in the script. Set them as env vars beforehand,
# e.g. in .Renviron:
#   TIKTOK_CLIENT_KEY=xxxxxxxx
#   TIKTOK_CLIENT_SECRET=xxxxxxxx
auth_research(
  client_key    = Sys.getenv("TIKTOK_CLIENT_KEY"),
  client_secret = Sys.getenv("TIKTOK_CLIENT_SECRET")
)

# --- 2. List of accounts ("parties") to scrape ------------------------------
parties <- c(
  "account_handle_1",
  "account_handle_2",
  "account_handle_3"
  # ... add as many handles as you need (no "@", just the username)
)

# --- 3. Date window to search within -----------------------------------------
# tt_user_videos_api() automatically breaks this range into the API's
# required <=30-day search windows and pages through each one for you.
since_date <- as.Date("2020-01-01")
to_date    <- Sys.Date()

# --- 4. Pull every video for every account, one account at a time -----------
# possibly() means one account failing (e.g. private/banned/renamed handle)
# doesn't kill the whole run - it just returns an empty tibble for that one.
safe_user_videos <- possibly(
  function(user) {
    tt_user_videos_api(
      username  = user,
      since     = since_date,
      to        = to_date,
      fields    = "all",   # all available video-level metadata, incl. caption
      max_pages = 999,     # keep paging within each 30-day window until exhausted
      verbose   = TRUE
    )
  },
  otherwise = tibble()
)

videos_by_party <- parties |>
  set_names() |>
  map(safe_user_videos, .progress = TRUE)

# --- 5. Combine into a single data frame -------------------------------------
# names_to = "queried_account" keeps track of which handle each batch was
# fetched for, in addition to the API's own `username` field on each video.
all_videos <- videos_by_party |>
  list_rbind(names_to = "queried_account")

# --- 6. Save ------------------------------------------------------------------
write_csv(all_videos, "tiktok_videos_by_party.csv")
saveRDS(all_videos, "tiktok_videos_by_party.rds")

message(
  "Done: ", nrow(all_videos), " videos from ", length(parties), " accounts written to ",
  "tiktok_videos_by_party.csv"
)

# Notes:
# - If a run gets interrupted or you hit the daily quota, {traktok} caches
#   query progress (cache = TRUE by default inside tt_search_api); see
#   ?last_query / ?tt_search_api to resume rather than re-querying from
#   scratch.
# - "fields = 'all'" returns every documented field (caption/video_description,
#   counts, hashtags, effects, mentions, music_id, region_code, etc.). Pass a
#   character vector instead if you only need specific columns.
