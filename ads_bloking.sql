cd#!/bin/bash
# Stats from YouTube Ad Blocker Tracker
# dasnav.corp.google.com/v/adblock

# Columnio files:
# /cns/vl-d/home/youtube-ads-logs-raw/dasnav/visitor/YYYY/MM/DD
# For each day there are two filesets each, one is light_dasnav_data which
# is exported the dashboard, the other one is called dasnav_data.

# Translate inot go/caqstats-in-adevents style
# at the query-level, RPM = "(1000 * sum(if(trueview, 0, revenue)) /
# sum($RevClean and (not matched or billable))) rpm"
# query-level should be fine for that.
# age/gender profiles are at the query-level.

# There's no straightforward way to block ads in the apps.
# The data suggests that 99% of the mobile adblock rate is caused by UCBrowser
# as it has adblocking enabled by default and is top mobile browser.
# A proxy to look at per-browser slicing of your data and assume that
# UCBrowser users are blocking ads and all others are not.

########
SET accounting_group analytics-internal-processing-dev;
SET min_completion_ratio 1;
SET io_timeout 2400;
SET nest_join_schema true;
SET runtime_name dremel;
SET materialize_overwrite true;
SET materialize_owner_group analytics-internal-processing-dev;
SET run_as_mdb_account aredakov;
SET enable_gdrive true;


DEFINE TABLE stats_caq /namespace/display-ads-privacy/monitoring/stats/caq/stats/2017/02/*/*/ttl=100d/stats*;
MATERIALIZE '/cns/ig-d/home/aredakov/ads_blocking/revenue/data@50' AS
SELECT
  country,
  browser_class,
   CASE
    WHEN (browser_class = 'MobileApp'
      AND inventory = 'INVENTORY_GDN') THEN 'AdMob'
    WHEN (browser_class IN ('Desktop','MobileWeb','Webviews')
      AND inventory = 'INVENTORY_GDN') THEN 'AdSense'
    WHEN inventory = 'INVENTORY_ADX' THEN 'AdX'
  END AS inventory,
  CASE
    WHEN user_age = 125829409 THEN '0 to 17'
    WHEN user_age = 125829529 THEN '18 to 24'
    WHEN user_age = 125829649 THEN '25 to 34'
    WHEN user_age = 125829769 THEN '35 to 44'
    WHEN user_age = 125829889 THEN '45 to 54'
    WHEN user_age = 125830009 THEN '55 to 64'
    WHEN user_age = 125830129 THEN '65+'
  END AS age,
  revenue
FROM
(SELECT
  dimensions.country AS country,
  dimensions.inventory AS inventory,
  CASE
    WHEN dimensions.browser_class = 1 THEN 'Desktop'
    WHEN dimensions.browser_class = 2 THEN 'MobileWeb'
    WHEN dimensions.browser_class = 3 THEN 'MobileApp'
    WHEN dimensions.browser_class = 4 THEN 'Webviews'
  END AS browser_class,
  dimensions.user_age AS user_age,
  SUM(metrics.caq_clean.revenue) AS revenue,
FROM stats_caq
WHERE dimensions.inventory IN ('INVENTORY_GDN', 'INVENTORY_ADX')
  AND dimensions.user_gender IN ('FEMALE_LIST_ID','MALE_LIST_ID')
  AND dimensions.user_age IN (125829409, 125829529, 125829649, 125829769,
    125829889, 125830009, 125830129)
  AND dimensions.browser_class IN (1,2,3,4)
  AND dimensions.country IN ('BR', 'CA', 'DE', 'ES', 'FR', 'GB', 'IN', 'IT',
      'JP', 'KR', 'MX', 'NL', 'RU', 'SA', 'TH', 'TR', 'US', 'VN')
GROUP@500 BY 1,2,3,4);

MATERIALIZE '/cns/ig-d/home/aredakov/ads_blocking/total_revenue/data@50' AS
SELECT
  dimensions.country AS country,
  SUM(metrics.caq_clean.revenue) AS total_revenue,
FROM stats_caq
WHERE dimensions.inventory IN ('INVENTORY_GDN', 'INVENTORY_ADX')
  AND dimensions.user_gender IN ('FEMALE_LIST_ID','MALE_LIST_ID')
  AND dimensions.user_age IN (125829409, 125829529, 125829649, 125829769,
    125829889, 125830009, 125830129)
  AND dimensions.browser_class IN (1,2,3,4)
  AND dimensions.country IN ('BR', 'CA', 'DE', 'ES', 'FR', 'GB', 'IN', 'IT',
      'JP', 'KR', 'MX', 'NL', 'RU', 'SA', 'TH', 'TR', 'US', 'VN')
GROUP@50 BY 1;

MATERIALIZE '/cns/ig-d/home/aredakov/ads_blocking/adblocking_rates/data@50' AS
SELECT
  country,
  user_age AS age,
  platform AS browser_class,
  (SUM(users_5dlck_pings_no_success_5day) -
    SUM(users_5dlck_pings_no_success_with_ads_5day))
    / SUM(users_5dlck_pings_5day) AS adblockrate
FROM zoom.dnskre31
WHERE date >= '2017-01-15'
  AND cookie_creation_age IN ('10d', '360d', '5d', '90d', '15d', '180d',
    '30d')
  AND embeds_fraction IN ('0.00', '0.25', '0.50', '0.75')
  AND experiment IN ('none')
  AND country IN ('BR', 'CA', 'DE', 'ES', 'FR', 'GB', 'IN', 'IT',
      'JP', 'KR', 'MX', 'NL', 'RU', 'SA', 'TH', 'TR', 'US', 'VN')
  AND platform = 'Desktop'
  AND interface = 'Web'
GROUP BY 1,2,3;

DEFINE TABLE revenue /cns/ig-d/home/aredakov/ads_blocking/revenue/data*;
DEFINE TABLE total_revenue /cns/ig-d/home/aredakov/ads_blocking/total_revenue/data*;
MATERIALIZE '/cns/ig-d/home/aredakov/ads_blocking/rev_share/data@50' AS
SELECT
  age,
  a.country AS country,
  browser_class,
  inventory,
  (revenue / total_revenue) AS rev_share
FROM revenue a
JOIN@50 total_revenue b
ON a.country = b.country;

DEFINE TABLE rev_share /cns/ig-d/home/aredakov/ads_blocking/rev_share/data*;
DEFINE TABLE adblocking_rates /cns/ig-d/home/aredakov/ads_blocking/adblocking_rates/data*;
MATERIALIZE 'trix:/gdrive/aredakov/ads_blocking/blocking_stats.csv@1 header:true' AS
SELECT
  adblockrate,
  rev_share,
  a.age AS age,
  a.country AS country,
  a.browser_class AS browser_class,
  a.inventory AS inventory
FROM rev_share a
LEFT JOIN@50 adblocking_rates b
ON a.country = b.country AND a.browser_class = b.browser_class
  AND a.user_gender = b.user_gender AND a.age = b.age;

########
# Charts
library(ginstall)
library(gfile)
library(namespacefs)
library(rglib)
library(cfs)
library(dremel)
library(Hmisc)
library(ggplot2)
library(scales)
library(directlabels)
library(lubridate)
library(boot)
library(gmp)
library(MASS)
InitGoogle()
options("scipen"=100, "digits"=12)

myConn <- DremelConnect()
DremelSetMinCompletionRatio(myConn, 1.0)
DremelSetAccountingGroup(myConn,'urchin-processing-qa')
DremelSetMaterializeOwnerGroup(myConn, 'materialize-a-dremel')
DremelSetMaterializeOverwrite(myConn, TRUE)
DremelSetIOTimeout(myConn, 7200)

# Signedin vs NonSignedIn
DremelAddTableDef('stats_qem', '/cns/vs-d/home/display-ads-privacy-monitoring/stats/qem/stats/2017/03/10/*/ttl=100d/stats*',
  myConn, verbose=FALSE)
DremelAddTableDef('stats_qem_unmatched', '/namespace/display-ads-privacy/monitoring/stats/qem.unmatched/stats/2017/03/10/*/ttl=100d/stats*',
  myConn, verbose=FALSE)
DremelAddTableDef('stats_pqem', '/cns/vs-d/home/display-ads-privacy-monitoring/stats/pqem/stats/2017/03/10/*/ttl=100d/stats*',
  myConn, verbose=FALSE)
DremelAddTableDef('stats_pqem_unmatched', '/namespace/display-ads-privacy/monitoring/stats/pqem.unmatched/stats/2017/03/10/*/ttl=100d/stats*',
  myConn, verbose=FALSE)

aa <- DremelExecuteQuery("
    SELECT
      IF(signed_in = true, 'SignedIn','NonSignedIn') AS Status,
      browser_class,
      CASE
        WHEN user_age = 125829409 THEN '0 to 17'
        WHEN user_age = 125829529 THEN '18 to 24'
        WHEN user_age = 125829649 THEN '25 to 34'
        WHEN user_age = 125829769 THEN '35 to 44'
        WHEN user_age = 125829889 THEN '45 to 54'
        WHEN user_age = 125830009 THEN '55 to 64'
        WHEN user_age = 125830129 THEN '65+'
      END AS age,
      queries
    FROM
    (SELECT
      dimensions.signed_in AS signed_in,
      CASE
        WHEN dimensions.browser_class = 1 THEN 'Desktop'
        WHEN dimensions.browser_class = 2 THEN 'MobileWeb'
        WHEN dimensions.browser_class = 3 THEN 'MobileApp'
        WHEN dimensions.browser_class = 4 THEN 'Webviews'
      END AS browser_class,
      dimensions.user_age AS user_age,
      SUM(metrics.qem_requests.total) AS queries,
    FROM stats_qem, stats_qem_unmatched, stats_pqem, stats_pqem_unmatched
    WHERE dimensions.inventory IN ('INVENTORY_GDN', 'INVENTORY_ADX')
      AND dimensions.user_gender IN ('FEMALE_LIST_ID','MALE_LIST_ID')
      AND dimensions.user_age IN (125829409, 125829529, 125829649, 125829769,
        125829889, 125830009, 125830129)
      AND dimensions.browser_class IN (1)
      AND dimensions.country IN ('US')
    GROUP@500 BY 1,2,3)
;", myConn)

# 0.detection-prod.server.adsaver.qk.borg.google.com:4038/debug/overview
# Ad Blocking Rate Variance by pub.
v <- DremelExecuteQuery("
  SELECT
    property_id,
    (SUM(report_adblock_custom_pageview_count) /
    SUM(report_detectable_custom_pageview_count)) AS rate
  FROM zoom.dnso8jw7.10291
  WHERE time_usec >= 1488355200000000
  GROUP@50 BY 1
  # HAVING rate > 0
;", myConn)

ggplot(v, aes(x = rate, fill = 'red')) +
geom_density(alpha = 0.5) +
scale_x_continuous(labels = percent) +
theme(legend.position="none")

describe(v)
t.test(v$rate)

# experiment = "None" This will include all traffic.
# Singed_in Rate. is_gaia is the field that tells you whether the particular
# user (slice) has been signed in at some point during the day.
# Data a user is represented as (visitor_id or gaia_id, user_agent_string)
# so it does not map 1:1 with YouTube user.
# Users dropped if don't have enough adblock-related data to infer a status.
# The mobile data shows fake adblock rate. In reality it's much lower (<1%).
# Some mobile devices report web interface and not mweb.
# Ad Blocking Rate.
# The mobile data shows fake adblock rate. In reality it's much lower (<1%).
# Some mobile devices report web interface and not mweb.
b <- DremelExecuteQuery("
  SELECT
    date,
    user_age AS age,
    user_gender AS gender,
    (SUM(users_5dlck_pings_no_success_5day) -
      SUM(users_5dlck_pings_no_success_with_ads_5day))
      / SUM(users_5dlck_pings_5day) AS adblockrate
  FROM zoom.dnskre31
  WHERE date >= '2017-01-15'
    AND cookie_creation_age IN ('10d', '360d', '5d', '90d', '15d', '180d',
      '30d')
    AND embeds_fraction IN ('0.00', '0.25', '0.50', '0.75')
    AND experiment IN ('none')
    AND country IN ('US')
    AND platform = 'Desktop'
    AND interface = 'Web'
    AND user_age NOT IN ('Unknown','')
    AND user_gender NOT IN ('Unknown','')
  GROUP BY 1,2,3
;", myConn)

b[b == 0] <- NA

b$date <- as.Date(b$date)
pd <- position_dodge(.1)
ggplot(data = b, aes(x = date, y = adblockrate, colour = gender)) +
facet_grid(age ~ gender) +
geom_line() +
stat_smooth(position=pd, method="loess", fullrange=TRUE, size=1.2, span=.4) +
ylab("Ad Block Rate") +
theme(legend.position="none" ) +
scale_y_continuous(labels = percent) +
theme(axis.text.x=element_text(size=8, color="gray26", angle = 45, hjust = 1)) +
ggtitle("Country=US.YouTube. Desktop. Ad Block Rate. Since 15 Jan 2016") +
theme(plot.title = element_text(lineheight=.8))
xlab("")

DremelAddTableDef('stats_qem', '/cns/vs-d/home/display-ads-privacy-monitoring/stats/qem/stats/2017/02/09/*/ttl=100d/stats*',
  myConn, verbose=FALSE)
DremelAddTableDef('stats_qem_unmatched', '/namespace/display-ads-privacy/monitoring/stats/qem.unmatched/stats/2017/02/09/*/ttl=100d/stats*',
  myConn, verbose=FALSE)
DremelAddTableDef('stats_pqem', '/cns/vs-d/home/display-ads-privacy-monitoring/stats/pqem/stats/2017/02/09/*/ttl=100d/stats*',
  myConn, verbose=FALSE)
DremelAddTableDef('stats_pqem_unmatched', '/namespace/display-ads-privacy/monitoring/stats/pqem.unmatched/stats/2017/02/09/*/ttl=100d/stats*',
  myConn, verbose=FALSE)

q <- DremelExecuteQuery("
  SELECT
    inventory,
    CASE
      WHEN user_age = 125829409 THEN '=<17'
      WHEN user_age = 125829529 THEN '18-24'
      WHEN user_age = 125829649 THEN '25-34'
      WHEN user_age = 125829769 THEN '35-44'
      WHEN user_age = 125829889 THEN '45-54'
      WHEN user_age = 125830009 THEN '55-64'
      WHEN user_age = 125830129 THEN '>=65'
    END AS age,
    CASE
        WHEN browser_class = 1 THEN 'Desktop'
        WHEN browser_class = 2 THEN 'MobileWeb'
        WHEN browser_class = 3 THEN 'MobileApp'
        WHEN browser_class = 4 THEN 'Webviews'
    END AS browser_class,
    queries
  FROM
    (SELECT
      dimensions.country AS country,
      CASE
        WHEN (dimensions.browser_class = 3
          AND dimensions.inventory = 'INVENTORY_GDN') THEN 'AdMob'
        WHEN (dimensions.browser_class IN (1,2,4)
          AND dimensions.inventory = 'INVENTORY_GDN') THEN 'AdSense'
        WHEN dimensions.inventory = 'INVENTORY_ADX' THEN 'AdX'
      END AS inventory,
      dimensions.browser_class AS browser_class,
      dimensions.user_age AS user_age,
      SUM(metrics.qem_requests.total) AS queries
    FROM stats_qem, stats_qem_unmatched, stats_pqem, stats_pqem_unmatched
    WHERE dimensions.inventory IN ('INVENTORY_GDN', 'INVENTORY_ADX')
      AND dimensions.country = 'US'
      AND dimensions.browser_class IN (1,2,3,4)
      AND dimensions.user_gender IN (3638449, 3638569)
      AND dimensions.user_age IN (125829409,125829529,125829649,125829769,
        125829889,125830009,125830129)
    GROUP@500 BY 1,2,3,4)
;", myConn)

rhg_cols <- c("#771C19","#AA3929","#E25033","#F27314","#F8A31B","#E2C59F",
  "#B6C5CC","#8E9CA3","#556670","#000000")

ggplot(data = q, aes(x = age, y = queries/1000000, fill=age)) +
geom_bar(stat = "identity") +
coord_cartesian(ylim = c(0, 410)) +
facet_grid(browser_class ~ inventory, labeller = label_value) +
scale_fill_manual(values = rhg_cols) +
theme_bw() +
geom_text(aes(label=sprintf("%1.1f", queries/1000000)),vjust=-0.1,
  size=2.5) +
theme(legend.position="none") +
theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
ggtitle("Country = US. Queries. 09 Feb 2017") +
ylab("Queries, M") + xlab("")

#########
# Revenue Share.
DremelAddTableDef('stats_caq', '/namespace/display-ads-privacy/monitoring/stats/caq/stats/2017/02/09/*/ttl=100d/stats*',
  myConn, verbose=FALSE)

DremelAddTableDef('rev_share', '/cns/ig-d/home/aredakov/ads_blocking/rev_share/data*',
  myConn, verbose=FALSE)
DremelAddTableDef('adblocking_rates', '/cns/ig-d/home/aredakov/ads_blocking/adblocking_rates/data*',
  myConn, verbose=FALSE)

d <- DremelExecuteQuery("
SELECT
  adblockrate,
  rev_share,
  a.age AS age,
  a.country AS country,
  a.browser_class AS browser_class,
  a.inventory AS inventory
FROM rev_share a
LEFT JOIN@50 adblocking_rates b
ON a.country = b.country AND a.browser_class = b.browser_class
  AND a.age = b.age
WHERE a.country = 'US'
;", myConn)

rhg_cols <- c("#771C19","#AA3929","#E25033","#F27314","#F8A31B","#E2C59F",
  "#B6C5CC","#8E9CA3","#556670","#000000")

ggplot(data = d, aes(x = age, y = rev_share, fill=age)) +
geom_bar(stat = "identity") +
coord_cartesian(ylim = c(0, 0.25)) +
facet_grid(browser_class ~ inventory, labeller = label_value) +
scale_fill_manual(values = rhg_cols) +
theme_bw() +
geom_text(aes(label=sprintf("%1.2f%%", rev_share*100)),vjust=-0.1,
  size=2.5) +
scale_y_continuous(labels = percent) +
theme(legend.position="none") +
theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
ggtitle("Country = US. RevShare. 09 Feb 2017") +
ylab("RevShare, %") + xlab("")

ggplot(data = d, aes(x = age, y = adblockrate, fill=age)) +
geom_bar(stat = "identity") +
coord_cartesian(ylim = c(0, 0.5)) +
facet_grid(browser_class ~ inventory, labeller = label_value) +
scale_fill_manual(values = rhg_cols) +
theme_bw() +
geom_text(aes(label=sprintf("%1.2f%%", adblockrate*100)),vjust=-0.1,
  size=2.5) +
scale_y_continuous(labels = percent) +
theme(legend.position="none") +
theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
ggtitle("Country = US. AdBlockRate. Feb 2017") +
ylab("AdBlockRate, %") + xlab("")

ggplot(data = d, aes(x = age, y = (rev_share / (1 - adblockrate)) *adblockrate, fill=age)) +
geom_bar(stat = "identity") +
coord_cartesian(ylim = c(0, 0.02)) +
facet_grid(browser_class ~ inventory, labeller = label_value) +
scale_fill_manual(values = rhg_cols) +
theme_bw() +
geom_text(aes(label=sprintf("%1.2f%%", ((rev_share / (1 - adblockrate)) *adblockrate)*100)),vjust=-0.1,
  size=2.5) +
scale_y_continuous(labels = percent) +
theme(legend.position="none") +
theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
ggtitle("Country = US. PossiblyLostRev. Feb 2017") +
ylab("PossiblyLostRev, %") + xlab("")
