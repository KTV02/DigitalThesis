#!/usr/bin/env Rscript

# fips_run.R  FIPS runner
# - Reads sleep_episodes.csv with columns: user,start,end[,sleep_id] OR sleep_start/sleep_end
# - Parses & rounds to 5-min grid, builds FIPS timeline CSV
# - Always writes: Sleep barcode PNG
# - Always writes: TPM & Unified model PNGs by opening a PNG device BEFORE calling FIPS_plot()

# IMPORTANT: the CSV contains local wall-clock times per record (already adjusted in Python).

suppressPackageStartupMessages({})

# ---------- helpers ----------
ensure_pkgs <- function(pkgs, repo = "https://cloud.r-project.org") {
  github_map <- c(FIPS = "humanfactors/FIPS")

  have_pkg <- function(x) requireNamespace(x, quietly = TRUE)
  missing <- pkgs[!vapply(pkgs, have_pkg, logical(1))]
  if (!length(missing)) return(invisible(TRUE))

  cran_targets <- setdiff(missing, names(github_map))
  if (length(cran_targets)) {
    message("[INFO] Installing missing R packages: ", paste(cran_targets, collapse = ", "))
    install.packages(cran_targets, repos = repo)
  }

  missing <- pkgs[!vapply(pkgs, have_pkg, logical(1))]
  github_targets <- intersect(missing, names(github_map))
  if (length(github_targets)) {
    if (!have_pkg("remotes")) {
      message("[INFO] Installing remotes to obtain GitHub packages")
      install.packages("remotes", repos = repo)
    }
    for (pkg in github_targets) {
      message("[INFO] Installing ", pkg, " from GitHub: ", github_map[[pkg]])
      remotes::install_github(github_map[[pkg]], upgrade = "never", dependencies = TRUE, build_vignettes = FALSE)
    }
  }

  missing <- pkgs[!vapply(pkgs, have_pkg, logical(1))]
  if (length(missing)) stop("Packages not installed: ", paste(missing, collapse = ", "))

  invisible(TRUE)
}

shift_tod <- function(x, cut = 18) {
  # x in hours [0,24); returns hours in [cut, cut+24)
  ifelse(x < cut, x + 24, x)
}

ensure_pkgs(c("optparse","readr","dplyr","lubridate","stringr","ggplot2","FIPS","patchwork"))

suppressPackageStartupMessages({
  library(optparse)
  library(readr)
  library(dplyr)
  library(lubridate)
  library(stringr)
  library(ggplot2)
  library(FIPS)
})

# macOS/headless-safe PNG device
open_png <- function(filename, width = 10, height = 6, res = 200) {
  dir.create(dirname(filename), showWarnings = FALSE, recursive = TRUE)
  if (isTRUE(capabilities("cairo"))) {
    grDevices::png(filename = filename, width = width, height = height,
                   units = "in", res = res, type = "cairo")
  } else {
    grDevices::png(filename = filename, width = width, height = height,
                   units = "in", res = res)
  }
}

# Save whatever FIPS_plot draws (it may return NULL)
save_fips_plot <- function(filename, plot_code, width_in = 10, height_in = 6, res = 200) {
  open_png(filename, width = width_in, height = height_in, res = res)
  ok <- TRUE
  tryCatch({
    invisible(plot_code())
  }, error = function(e) {
    ok <<- FALSE
    message("[WARN] Plotting failed for ", basename(filename), ": ", conditionMessage(e))
  }, finally = {
    grDevices::dev.off()
  })
  if (ok && file.exists(filename)) {
    message("[OK] Wrote plot -> ", filename)
  } else {
    message("[WARN] No image written: ", filename)
  }
}



# ---- Date-labeled S curve from a FIPS simulation ----
plot_S_by_date <- function(sim_df, t0, outfile, title = "Homeostatic S (by date)") {
  # Ensure types are clean and monotonic in time
df <- sim_df %>%
  dplyr::mutate(
    sim_hours = as.numeric(.data$sim_hours),
    s         = as.numeric(.data$s)
  ) %>%
  dplyr::filter(
    !is.na(sim_hours),
    !is.na(s)
  ) %>%
  dplyr::arrange(sim_hours) %>%
  dplyr::transmute(
    datetime = as.POSIXct(t0, tz = "UTC") + lubridate::dhours(sim_hours),
    S = s
  )

  span_days <- as.numeric(difftime(max(df$datetime), min(df$datetime), units = "days"))
  date_step <- if (span_days > 56) "2 weeks" else if (span_days > 28) "1 week" else "3 days"

  mids <- seq(lubridate::floor_date(min(df$datetime), "day") + lubridate::days(1),
              lubridate::ceiling_date(max(df$datetime), "day"),
              by = "1 day")

  p <- ggplot(df, aes(datetime, S)) +
    geom_line(linewidth = 0.6) +
    geom_vline(xintercept = as.numeric(mids), linetype = "dashed", alpha = 0.15) +
    scale_x_datetime(date_labels = "%b %d", date_breaks = date_step,
                     expand = expansion(mult = c(.01, .02))) +
    labs(x = "Date (local-at-source)", y = "Homeostatic S", title = title) +
    theme_minimal(base_size = 13)

  ggplot2::ggsave(outfile, p, width = 12, height = 6, dpi = 180)
  message("[OK] Wrote date-labeled plot -> ", outfile)
}


# ---------- CLI ----------
opt_list <- list(
  make_option("--sleep_csv", type="character", help="Path to sleep_episodes.csv"),
  make_option("--out_dir",   type="character", help="Output directory for plots/exports"),
  make_option("--user_id",   type="character", default="USER123", help="User id label"),
  make_option("--plots",     type="character", default="three_process,unified_model",
              help="Comma-separated: three_process, unified_model (ignored here; both run)")
)
opt <- parse_args(OptionParser(option_list = opt_list), positional_arguments = FALSE)
if (is.null(opt$sleep_csv) || is.null(opt$out_dir)) stop("Missing --sleep_csv or --out_dir")
dir.create(opt$out_dir, showWarnings = FALSE, recursive = TRUE)

message("[START] fips_run.R")
message("[INFO] Using FIPS version: ", as.character(utils::packageVersion("FIPS")))
message("[INFO] Loading sleep CSV: ", opt$sleep_csv)

# ---------- load & normalize input ----------
raw <- readr::read_csv(opt$sleep_csv, show_col_types = FALSE)
cols <- names(raw)
start_col <- intersect(c("sleep_start","start"), cols)
end_col   <- intersect(c("sleep_end","end"), cols)
if (!length(start_col) || !length(end_col)) {
  stop("Input must contain either (sleep_start, sleep_end) or (start, end). Found: ",
       paste(cols, collapse=", "))
}
start_col <- start_col[[1]]
end_col   <- end_col[[1]]

if (!"user" %in% cols) raw$user <- opt$user_id
raw$user <- ifelse(is.na(raw$user) | trimws(raw$user) == "", opt$user_id, raw$user)

parse_vec <- function(x) {
  x <- as.character(x)
  #interpret the CSV strings in a neutral zone (UTC tag) so the clock
  #values stay exactly as written; do NOT convert them.
  dt <- suppressWarnings(ymd_hms(x, tz = "UTC", quiet = TRUE))

  if (any(is.na(dt))) {
    alt <- suppressWarnings(lubridate::parse_date_time(
      x, orders = c("Y-m-d H:M:S","Y-m-d H:M","Y/m/d H:M:S","Y/m/d H:M",
                    "d.m.Y H:M:S","d.m.Y H:M","m/d/Y H:M:S","m/d/Y H:M"),
      tz = "UTC", quiet = TRUE
    ))
    dt[is.na(dt)] <- alt[is.na(dt)]
  }
  dt
}
start_local <- parse_vec(raw[[start_col]])
end_local   <- parse_vec(raw[[end_col]])

ok <- !is.na(start_local) & !is.na(end_local) & (end_local > start_local)
raw <- raw[ok, , drop = FALSE]
start_local <- start_local[ok]
end_local   <- end_local[ok]

if (!nrow(raw)) stop("No valid sleep episodes after parsing times.")

norm <- tibble::tibble(
  user     = raw$user,
  start    = lubridate::floor_date(start_local, unit = "5 minutes"),
  end      = lubridate::ceiling_date(end_local, unit = "5 minutes"),
  sleep_id = if ("sleep_id" %in% names(raw)) suppressWarnings(as.integer(raw$sleep_id)) else NA_integer_
) %>% filter(end > start)

# Sanity check: rounding should move starts backward 0–<5 min and ends forward 0–<5 min
delta_start <- as.numeric(difftime(start_local, norm$start, units = "mins"))
delta_end   <- as.numeric(difftime(norm$end,   end_local,   units = "mins"))

bad_idx <- which(delta_start < 0 | delta_start >= 5 | delta_end < 0 | delta_end >= 5)
if (length(bad_idx)) {
  warning(sprintf(
    "Rounding sanity check: %d rows moved more than expected (showing up to 5):\n%s",
    length(bad_idx),
    paste(utils::capture.output(print(
      data.frame(
        start_orig = start_local[bad_idx],
        start_round = norm$start[bad_idx],
        end_orig = end_local[bad_idx],
        end_round = norm$end[bad_idx],
        d_start_min = sprintf("%.2f", delta_start[bad_idx]),
        d_end_min = sprintf("%.2f", delta_end[bad_idx])
      )[seq_len(min(5, length(bad_idx))), ]
    )), collapse = "\n")
  ))
} 

# Fill missing sleep_id
if (all(is.na(norm$sleep_id))) {
  norm$sleep_id <- seq_len(nrow(norm))
} else {
  na_idx <- which(is.na(norm$sleep_id))
  if (length(na_idx)) {
    norm$sleep_id[na_idx] <- seq_len(length(na_idx)) + max(c(0L, norm$sleep_id), na.rm = TRUE)
  }
}
norm <- arrange(norm, start)

series.start <- min(norm$start, na.rm = TRUE)
series.end   <- max(norm$end,   na.rm = TRUE)
span_mins <- as.numeric(difftime(series.end, series.start, units="mins"))
message(sprintf("[DEBUG] series.start (clock-tagged UTC): %s", format(series.start, "%Y-%m-%d %H:%M:%S %Z")))
message(sprintf("[DEBUG] series.end (clock-tagged UTC): %s", format(series.end, "%Y-%m-%d %H:%M:%S %Z")))
message("[DEBUG] span (mins):  ", span_mins)

# ---------- Build FIPS timeline ----------
st_df <- norm %>% select(sleep_id, start, end)

FIPS_df <- FIPS::sleeptimes_to_FIPSdf(
  sleeptimes      = st_df,
  series.start    = series.start,
  series.end      = series.end,
  roundvalue      = 5,          # minutes (>1)
  sleep.start.col = "start",
  sleep.end.col   = "end",
  sleep.id.col    = "sleep_id"
)

timeline_csv <- file.path(opt$out_dir, paste0("FIPS_timeline_", opt$user_id, ".csv"))
readr::write_csv(FIPS_df, timeline_csv)
message("[OK] Wrote parsed FIPS timeline -> ", timeline_csv)

readr::write_csv(norm %>% select(user, start, end, sleep_id),
                 file.path(opt$out_dir, paste0("sleep_episodes_rounded_", opt$user_id, ".csv")))

# ---------- Guaranteed sleep barcode ----------
stopifnot(inherits(FIPS_df$datetime, "POSIXct"))
barcode_png <- file.path(opt$out_dir, paste0("Sleep_Barcode_", opt$user_id, ".png"))
p_bar <- ggplot(FIPS_df, aes(x = as.POSIXct(datetime), y = 1, fill = wake_status)) +
  geom_tile(height = 0.9) +
  scale_fill_manual(values = c("FALSE" = "black", "TRUE" = "white")) +
  labs(x = "Time (local-at-source)", y = NULL, fill = "Awake?") +
  theme_minimal(base_size = 12) +
  theme(axis.text.y = element_blank(),
        axis.ticks.y = element_blank(),
        legend.position = "bottom")
ggsave(barcode_png, p_bar, width = 14, height = 1.8, dpi = 150)
message("[OK] Sleep barcode -> ", barcode_png)



# ------------------------- TPM -------------------------
message("[INFO] Simulating Three-Process Model (TPM)...")
pvec_tpm <- FIPS::TPM_make_pvec()
sim_tpm  <- FIPS::FIPS_simulate(FIPS_df, modeltype = "TPM", pvec = pvec_tpm)

# De-duplicate any end-of-day collisions: keep the last row per (day, time)
sim_tpm <- sim_tpm %>%
  dplyr::arrange(.data$day, .data$time) %>%
  dplyr::group_by(.data$day, .data$time) %>%
  dplyr::slice_tail(n = 1) %>%
  dplyr::ungroup()

tpm_png  <- file.path(opt$out_dir, paste0("TPM_", opt$user_id, ".png"))

ok <- TRUE
tryCatch({
  save_fips_plot(tpm_png, function() FIPS::FIPS_plot(sim_tpm), width_in = 12, height_in = 6, res = 180)
}, error = function(e) {
  ok <<- FALSE
  message("[WARN] FIPS_plot(TPM) failed: ", conditionMessage(e))
})

if (!file.exists(tpm_png)) {
  message("[INFO] Writing fallback TPM curve...")
  dfp <- sim_tpm %>% dplyr::mutate(t = .data$sim_hours)
  open_png(tpm_png, width = 12, height = 6, res = 180)
  try({
    plot(dfp$t, dfp$s, type = "l", xlab = "Hours since start", ylab = "Homeostatic S",
         main = "TPM – Fallback fatigue curve")
  }, silent = TRUE)
  grDevices::dev.off()
  if (file.exists(tpm_png)) message("[OK] Wrote fallback -> ", tpm_png)
}


# Also write a date-labeled version of the S curve
tpm_dates_png <- file.path(opt$out_dir, paste0("TPM_dates_", opt$user_id, ".png"))
plot_S_by_date(sim_tpm, series.start, tpm_dates_png, "TPM — Homeostatic S (by date)")


# ------------------------- Unified -------------------------
message("[INFO] Simulating Unified Model...")
pvec_u <- FIPS::unified_make_pvec()
sim_u  <- FIPS::FIPS_simulate(FIPS_df, modeltype = "unified", pvec = pvec_u)

sim_u <- sim_u %>%
  dplyr::arrange(.data$day, .data$time) %>%
  dplyr::group_by(.data$day, .data$time) %>%
  dplyr::slice_tail(n = 1) %>%
  dplyr::ungroup()

u_png  <- file.path(opt$out_dir, paste0("Unified_", opt$user_id, ".png"))

ok <- TRUE
tryCatch({
  save_fips_plot(u_png, function() FIPS::FIPS_plot(sim_u), width_in = 12, height_in = 6, res = 180)
}, error = function(e) {
  ok <<- FALSE
  message("[WARN] FIPS_plot(Unified) failed: ", conditionMessage(e))
})

if (!file.exists(u_png)) {
  message("[INFO] Writing fallback Unified curve (S only)...")
  dfp <- sim_u %>% dplyr::mutate(t = .data$sim_hours)
  open_png(u_png, width = 12, height = 6, res = 180)
  try({
    plot(dfp$t, dfp$s, type = "l", xlab = "Hours since start", ylab = "Homeostatic S",
         main = "Unified – Fallback fatigue curve")
  }, silent = TRUE)
  grDevices::dev.off()
  if (file.exists(u_png)) message("[OK] Wrote fallback -> ", u_png)
}

# Also write a date-labeled version of the S curve
u_dates_png <- file.path(opt$out_dir, paste0("Unified_dates_", opt$user_id, ".png"))
plot_S_by_date(sim_u, series.start, u_dates_png, "Unified — Homeostatic S (by date)")

# ---------- Bedtime/Wake + Sleep Duration (combined) ----------
combo_png <- file.path(opt$out_dir, paste0("Sleep_Timing_and_Duration_", opt$user_id, ".png"))

# Derive per-night metrics from the already rounded 'norm' table
btw <- norm %>%
  mutate(
    day_label = as.Date(end),
    duration_h = as.numeric(difftime(end, start, units = "hours"))
  ) %>%
  group_by(day_label) %>%
  slice_max(order_by = duration_h, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  mutate(
    start_local = start,
    end_local   = end,
    bedtime_tod = hour(start_local) + minute(start_local)/60,
    wake_tod    = hour(end_local)   + minute(end_local)/60,
    bedtime_plot = shift_tod(bedtime_tod, cut = 18),
    wake_plot    = shift_tod(wake_tod,    cut = 18)
  ) %>%
  arrange(day_label)

# Top panel: sleep timing (bedtime & wake as local time-of-day)
p_timing <- ggplot(btw, aes(x = day_label)) +
  geom_point(aes(y = bedtime_plot, color = "Bedtime"), size = 2) +
  geom_point(aes(y = wake_plot,    color = "Wake"),    size = 2) +
  scale_y_continuous(
    limits = c(18, 42),
    breaks = seq(18, 42, 4),
    labels = function(x) sprintf("%02d:00", (x %% 24))
  ) +
  scale_color_manual(values = c("Bedtime" = "#1f77b4", "Wake" = "#d62728"), name = NULL) +
  labs(
    title = "Sleep timing (bedtime & wake)",
    x = NULL, y = "Local clock time (local-at-source)"
  ) +
  theme_minimal(base_size = 12) +
  theme(legend.position = "bottom")

# Bottom panel: duration with smooth trend
p_duration <- ggplot(btw, aes(x = day_label, y = duration_h)) +
  geom_col(width = 0.9, alpha = 0.9) +
  geom_smooth(se = FALSE, method = "loess", span = 0.4) +
  labs(
    title = "Sleep duration with trend",
    x = NULL, y = "Sleep duration (hours)"
  ) +
  theme_minimal(base_size = 12)

# Combine and save
ok <- FALSE
if (requireNamespace("patchwork", quietly = TRUE)) {
  combined <- p_timing / p_duration +
    patchwork::plot_layout(heights = c(2, 1)) +
    patchwork::plot_annotation(title = "Sleep timing & duration")
  ggplot2::ggsave(combo_png, combined, width = 14, height = 7, dpi = 150)
  ok <- TRUE
} else {
  # Fallback: base grid (no extra packages)
  g1 <- ggplotGrob(p_timing); g2 <- ggplotGrob(p_duration)
  grDevices::png(combo_png, width = 14, height = 7, units = "in", res = 150)
  grid::grid.newpage()
  grid::pushViewport(grid::viewport(
    layout = grid::grid.layout(nrow = 2, ncol = 1, heights = grid::unit(c(2, 1), "null"))
  ))
  grid::grid.draw(g1, vp = grid::viewport(layout.pos.row = 1, layout.pos.col = 1))
  grid::grid.draw(g2, vp = grid::viewport(layout.pos.row = 2, layout.pos.col = 1))
  grDevices::dev.off()
  ok <- TRUE
}
if (ok) message("[OK] Timing+Duration plot -> ", combo_png)

# ---------- Additional Circadian Stability Metrics ----------

# Convert episodes to local time
btw_mid <- norm %>%
  dplyr::mutate(
    date = as.Date(end),  # wake-date anchor
    duration_h = as.numeric(difftime(end, start, units = "hours"))
  ) %>%
  dplyr::group_by(date) %>%
  dplyr::slice_max(order_by = duration_h, n = 1, with_ties = FALSE) %>%  # main sleep only
  dplyr::ungroup() %>%
  dplyr::mutate(
    midpoint = start + (end - start)/2,
    midpoint_tod = lubridate::hour(midpoint) + lubridate::minute(midpoint)/60,
    midpoint_plot = shift_tod(midpoint_tod, cut = 18)
  ) %>%
  dplyr::arrange(date)

# 1) Sleep Midpoint Time-Series -----------------------------------------
mid_png <- file.path(opt$out_dir, paste0("Sleep_Midpoint_", opt$user_id, ".png"))
p_mid <- ggplot(btw_mid, aes(x = date, y = midpoint_plot, group = 1)) +
  geom_line() +
  geom_point(size = 2) +
  scale_y_continuous(
    limits = c(18, 42),
    breaks = seq(18, 42, 4),
    labels = function(x) sprintf("%02d:00", (x %% 24))
  ) +
  labs(
    title = "Daily Sleep Midpoint (Local Time)",
    x = NULL, y = "Clock Time (local-at-source)"
  ) +
  theme_minimal(base_size = 12)

ggsave(mid_png, p_mid, width = 12, height = 4, dpi = 150)
message("[OK] Sleep midpoint plot -> ", mid_png)

# 2) Sleep Regularity Index (SRI) ----------------------------------------
# Phillips et al. 2017: proportion of matching sleep/wake state across 48-hour paired timepoints

compute_SRI <- function(FIPS_df) {
  df <- FIPS_df %>%
    mutate(day = as.Date(datetime)) %>%
    group_by(day) %>%
    mutate(idx = row_number()) %>%
    ungroup()

  df_lag <- df %>%
    mutate(day = day + 1)

  df_join <- dplyr::inner_join(df, df_lag, by = c("day","idx"), suffix = c("_d0","_d1"))
  mean(df_join$wake_status_d0 == df_join$wake_status_d1, na.rm = TRUE) * 100
}

SRI <- compute_SRI(FIPS_df)
writeLines(sprintf("Sleep Regularity Index (SRI): %.1f", SRI),
           file.path(opt$out_dir, paste0("SRI_", opt$user_id, ".txt")))
message(sprintf("[OK] SRI = %.1f (saved to text file)", SRI))

# 3) Sleep Duration Distribution -----------------------------------------
dur_png <- file.path(opt$out_dir, paste0("Sleep_Duration_Distribution_", opt$user_id, ".png"))
p_dur <- ggplot(btw, aes(x = duration_h)) +
  geom_histogram(binwidth = 0.5, fill = "#4C72B0", color = "black", alpha = 0.8) +
  labs(title = "Distribution of Sleep Duration",
       x = "Sleep duration (hours)", y = "N nights") +
  theme_minimal(base_size = 12)
ggsave(dur_png, p_dur, width = 8, height = 4, dpi = 150)
message("[OK] Duration histogram -> ", dur_png)

message("[DONE] fips_run.R finished.")