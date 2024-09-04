extern crate charming;
extern crate chrono;
extern crate polars;

use charming::{
    component::{Axis, Grid, Legend, Title},
    element::{AreaStyle, ItemStyle, LineStyle},
    series::Line,
    Chart, ImageFormat, ImageRenderer,
};
use polars::prelude::*;
use std::error::Error;

fn read_exported_dexcom_values(file_path: &str) -> PolarsResult<DataFrame> {
    CsvReadOptions::default()
        .with_has_header(true)
        .try_into_reader_with_file_path(Some(file_path.into()))?
        .finish()
}

fn clean_data(df: DataFrame) -> PolarsResult<DataFrame> {
    let timestamp_col = "Timestamp (YYYY-MM-DDThh:mm:ss)";
    let glucose_col = "Glucose Value (mg/dL)";

    // Select only the timestamp and glucose value columns
    let only_ts_val = df.select([timestamp_col, glucose_col])?;

    // Replace "Low" with 30, convert to i32, and replace negative values with null
    let cleaned_df = only_ts_val
        .lazy()
        .with_column(
            when(col(glucose_col).eq(lit("Low")))
                .then(lit(30))
                .otherwise(col(glucose_col).cast(DataType::Int32))
                .alias(glucose_col),
        )
        .with_column(
            when(col(glucose_col).lt(lit(0)))
                .then(lit(NULL))
                .otherwise(col(glucose_col))
                .alias(glucose_col),
        )
        .with_column(col(timestamp_col).str().strptime(
            DataType::Datetime(TimeUnit::Milliseconds, None),
            StrptimeOptions {
                format: Some("%Y-%m-%dT%H:%M:%S".to_string()),
                strict: false,
                ..Default::default()
            },
            lit("raise"),
        ))
        .collect()?;

    return cleaned_df.drop_nulls::<String>(None);
}

fn calculate_hourly_stats(df: DataFrame) -> PolarsResult<DataFrame> {
    let timestamp_col = "Timestamp (YYYY-MM-DDThh:mm:ss)";
    let glucose_col = "Glucose Value (mg/dL)";

    df.lazy()
        .with_column(col(timestamp_col).dt().hour().alias("Hour"))
        .group_by([col("Hour")])
        .agg([
            col(glucose_col).mean().alias("Mean Glucose Value"),
            col(glucose_col)
                .quantile(lit(0.05), QuantileInterpolOptions::Nearest)
                .alias("5th Percentile"),
            col(glucose_col)
                .quantile(lit(0.25), QuantileInterpolOptions::Nearest)
                .alias("25th Percentile"),
            col(glucose_col)
                .quantile(lit(0.75), QuantileInterpolOptions::Nearest)
                .alias("75th Percentile"),
            col(glucose_col)
                .quantile(lit(0.95), QuantileInterpolOptions::Nearest)
                .alias("95th Percentile"),
        ])
        .sort(
            ["Hour"],
            SortMultipleOptions {
                descending: vec![false],
                nulls_last: vec![false],
                multithreaded: true,
                maintain_order: false,
            },
        )
        .collect()
}

fn plot_hourly_stats(hourly_stats: DataFrame) -> Result<Chart, Box<dyn Error>> {
    let hours: Vec<String> = hourly_stats["Hour"]
        .i8()?
        .into_no_null_iter()
        .map(|hour| hour.to_string())
        .collect();
    let mean_values: Vec<f64> = hourly_stats["Mean Glucose Value"]
        .f64()?
        .into_no_null_iter()
        .collect();
    let percentile_5: Vec<f64> = hourly_stats["5th Percentile"]
        .f64()?
        .into_no_null_iter()
        .collect();
    let percentile_25: Vec<f64> = hourly_stats["25th Percentile"]
        .f64()?
        .into_no_null_iter()
        .collect();
    let percentile_75: Vec<f64> = hourly_stats["75th Percentile"]
        .f64()?
        .into_no_null_iter()
        .collect();
    let percentile_95: Vec<f64> = hourly_stats["95th Percentile"]
        .f64()?
        .into_no_null_iter()
        .collect();
    let area_25_75: Vec<f64> = percentile_25
        .iter()
        .zip(percentile_75.iter())
        .map(|(l, h)| h - l)
        .collect();
    let area_5_95: Vec<f64> = percentile_5
        .iter()
        .zip(percentile_95.iter())
        .map(|(l, h)| h - l)
        .collect();
    let max_value = percentile_95.clone().into_iter().reduce(f64::max).unwrap();

    return Ok(Chart::new()
        .title(Title::new().text("Hourly Mean Glucose Levels"))
        .x_axis(Axis::new().name("Hour of the Day").data(hours))
        .y_axis(
            Axis::new()
                .name("Glucose Value (mg/dL)")
                .interval(25)
                .min(0)
                .max((max_value + 50.0) - (max_value % 25.0)), // adjust the maximum of the graph to the y grid interval
        )
        .legend(Legend::new().top("bottom"))
        .background_color("#fff")
        .grid(Grid::new())
        .series(
            // Draw the band of 5 to 95 percentile interval
            Line::new()
                .data(percentile_5.clone())
                .line_style(LineStyle::new().opacity(0))
                .stack("confidence-5-95-band")
                .smooth(0.5)
                .show_symbol(false),
        )
        .series(
            Line::new()
                .data(area_5_95)
                .line_style(LineStyle::new().opacity(0))
                .area_style(AreaStyle::new().color("#ddd").opacity(0.5))
                .stack("confidence-5-95-band")
                .smooth(0.5)
                .show_symbol(false),
        )
        .series(
            // Draw the 5th percentile line
            Line::new()
                .name("5th Percentile")
                .data(percentile_5)
                .item_style(ItemStyle::new().opacity(0))
                .line_style(LineStyle::new().color("#d33"))
                .smooth(0.5),
        )
        .series(
            // Draw the 95th percentile line
            Line::new()
                .name("95th Percentile")
                .data(percentile_95)
                .item_style(ItemStyle::new().opacity(0))
                .line_style(LineStyle::new().color("#833"))
                .smooth(0.5),
        )
        .series(
            // Draw the band of 25 to 75 percentile interval
            Line::new()
                .data(percentile_25.clone())
                .line_style(LineStyle::new().opacity(0))
                .stack("confidence-25-75-band")
                .smooth(0.5)
                .show_symbol(false),
        )
        .series(
            Line::new()
                .data(area_25_75)
                .line_style(LineStyle::new().opacity(0))
                .area_style(AreaStyle::new().color("#ccc").opacity(0.65))
                .stack("confidence-25-75-band")
                .smooth(0.5)
                .show_symbol(false),
        )
        .series(
            // Draw the 25th percentile line
            Line::new()
                .name("25th Percentile")
                .data(percentile_25)
                .item_style(ItemStyle::new().opacity(0))
                .line_style(LineStyle::new().color("#33d"))
                .smooth(0.5),
        )
        .series(
            // Draw the 75th percentile line
            Line::new()
                .name("75th Percentile")
                .data(percentile_75)
                .item_style(ItemStyle::new().opacity(0))
                .line_style(LineStyle::new().color("#338"))
                .smooth(0.5),
        )
        .series(
            // Draw the mean line
            Line::new()
                .name("Mean Glucose")
                .data(mean_values)
                .item_style(ItemStyle::new().opacity(0))
                .line_style(LineStyle::new().color("#4d4"))
                .smooth(0.5),
        ));
}

fn save_chart_as_file(chart: Chart) -> Result<(), Box<dyn Error>> {
    let file_name = "glucose_levels.png";

    ImageRenderer::new(1400, 800)
        .save_format(ImageFormat::Png, &chart, file_name)
        .map_err(|err| format!("Error rendering the chart: {:?}", err))?;

    println!("Plot has been saved as {file_name}");

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <csv_file_path>", args[0]);
        std::process::exit(1);
    }

    let file_path = &args[1];

    let raw_values = read_exported_dexcom_values(file_path)?;
    let glucose_levels = clean_data(raw_values)?;
    let hourly_stats = calculate_hourly_stats(glucose_levels)?;
    let hourly_chart = plot_hourly_stats(hourly_stats)?;
    save_chart_as_file(hourly_chart)?;

    Ok(())
}
