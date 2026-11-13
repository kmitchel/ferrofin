//! Local Linux DVB capture using the dvbv5-zap utility from v4l-utils.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use ferrofin_model::dto::MediaSourceInfo;
use ferrofin_model::live_tv::TunerHostInfo;
use ferrofin_traits::error::ServiceError;
use tokio::io::AsyncReadExt as _;
use tokio::process::{Child, ChildStdout, Command};

use crate::stream::{self, TunerStreamBody};
use crate::tuner_host::{ChannelStream, ChannelStreamKind, StoredChannel, TunerChannel, TunerHost};

const DEVICE_ROOT: &str = "/dev/dvb";
const TUNE_SECTION: &str = "ferrofin";
const LINEUP_LIMIT: u64 = 1024 * 1024;

#[derive(Debug, Default)]
pub(crate) struct LinuxDvbHost {
    in_use: Arc<Mutex<BTreeSet<u32>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Selects one station from a configured dvbv5 lineup.
pub struct DvbStreamPlan {
    lineup: PathBuf,
    number: String,
}

struct Channel<'a> {
    name: &'a str,
    section: &'a str,
    service: u16,
}

fn parse_lineup(input: &str) -> Result<BTreeMap<&str, Channel<'_>>, ServiceError> {
    let mut result = BTreeMap::new();
    let starts = input.match_indices('\n').map(|(index, _)| index + 1);
    let section_offsets: Vec<_> = std::iter::once(0)
        .chain(starts)
        .filter(|&offset| {
            let line = input[offset..].lines().next().unwrap_or_default().trim();
            line.starts_with('[') && line.ends_with(']')
        })
        .collect();
    for (start, end) in section_offsets
        .iter()
        .copied()
        .zip(section_offsets.iter().copied().skip(1).chain([input.len()]))
    {
        let section = &input[start..end];
        let mut lines = section.lines();
        let name = lines
            .next()
            .unwrap_or_default()
            .trim()
            .trim_start_matches('[')
            .trim_end_matches(']');
        let fields: BTreeMap<_, _> = lines
            .filter_map(|line| line.split_once('='))
            .map(|(key, value)| (key.trim(), value.trim()))
            .collect();
        let (Some(&number), Some(&frequency), Some(&service)) = (
            fields.get("VCHANNEL"),
            fields.get("FREQUENCY"),
            fields.get("SERVICE_ID"),
        ) else {
            continue;
        };
        let service = service
            .strip_prefix("0x")
            .or_else(|| service.strip_prefix("0X"))
            .map_or_else(|| service.parse(), |hex| u16::from_str_radix(hex, 16));
        if number.is_empty() || frequency.parse::<std::num::NonZeroU64>().is_err() {
            return Err(ServiceError::invalid_input(
                "Invalid DVB channel number or frequency",
            ));
        }
        let service = service.map_err(|_| ServiceError::invalid_input("Invalid DVB service ID"))?;
        if result
            .insert(
                number,
                Channel {
                    name,
                    section,
                    service,
                },
            )
            .is_some()
        {
            return Err(ServiceError::invalid_input(format!(
                "Duplicate DVB channel number {number}"
            )));
        }
    }
    if result.is_empty() {
        return Err(ServiceError::invalid_input(
            "No DVB channels with VCHANNEL, FREQUENCY and SERVICE_ID",
        ));
    }
    Ok(result)
}

async fn read_lineup(path: &Path) -> Result<String, ServiceError> {
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(ServiceError::backend_source)?;
    if !metadata.is_file() || metadata.len() > LINEUP_LIMIT {
        return Err(ServiceError::invalid_input(
            "DVB lineup must be a regular file no larger than 1 MiB",
        ));
    }
    let mut text = String::new();
    tokio::fs::File::open(path)
        .await
        .map_err(ServiceError::backend_source)?
        .take(LINEUP_LIMIT + 1)
        .read_to_string(&mut text)
        .await
        .map_err(ServiceError::backend_source)?;
    if text.len() as u64 > LINEUP_LIMIT {
        return Err(ServiceError::invalid_input("DVB lineup exceeds 1 MiB"));
    }
    Ok(text)
}

fn lineup_path(tuner: &TunerHostInfo) -> Result<&Path, ServiceError> {
    tuner
        .url
        .as_deref()
        .map(Path::new)
        .filter(|path| path.is_absolute())
        .ok_or_else(|| {
            ServiceError::invalid_input("Linux DVB requires an absolute dvbv5 lineup file path")
        })
}

fn source_for(tuner: &TunerHostInfo, channel: &StoredChannel) -> MediaSourceInfo {
    let mut source = stream::create_media_source_info(&channel.external_id, tuner, false);
    source.path = None;
    source.path_is_private = true;
    source.container = Some("ts".to_owned());
    source.supports_direct_play = false;
    source.supports_direct_stream = true;
    source.requires_looping = false;
    source
}

#[async_trait]
impl TunerHost for LinuxDvbHost {
    fn name(&self) -> &'static str {
        "Linux DVB"
    }

    fn type_id(&self) -> &'static str {
        "linuxdvb"
    }

    async fn validate(&self, tuner: &mut TunerHostInfo) -> Result<(), ServiceError> {
        let path = tokio::fs::canonicalize(lineup_path(tuner)?)
            .await
            .map_err(ServiceError::backend_source)?;
        parse_lineup(&read_lineup(&path).await?)?;
        tuner.url = Some(path.to_string_lossy().into_owned());
        Ok(())
    }

    async fn get_channels(&self, tuner: &TunerHostInfo) -> Result<Vec<TunerChannel>, ServiceError> {
        let path = lineup_path(tuner)?;
        let text = read_lineup(path).await?;
        let lineup_id = ferrofin_common::extensions::get_md5(&path.to_string_lossy()).simple();
        Ok(parse_lineup(&text)?
            .into_iter()
            .map(|(number, channel)| TunerChannel {
                external_id: format!("linuxdvb_{lineup_id}_{number}"),
                guide_key: number.to_owned(),
                name: channel.name.to_owned(),
                number: Some(number.to_owned()),
                // An internal selector for this host; never handed to a client as a URL.
                url: number.to_owned(),
                ..TunerChannel::default()
            })
            .collect())
    }

    async fn channel_media_sources(
        &self,
        tuner: &TunerHostInfo,
        channel: &StoredChannel,
    ) -> Result<Vec<MediaSourceInfo>, ServiceError> {
        Ok(vec![source_for(tuner, channel)])
    }

    async fn channel_stream(
        &self,
        tuner: &TunerHostInfo,
        channel: &StoredChannel,
        _stream_id: Option<&str>,
    ) -> Result<ChannelStream, ServiceError> {
        Ok(ChannelStream {
            source: source_for(tuner, channel),
            kind: ChannelStreamKind::LinuxDvb(DvbStreamPlan {
                lineup: lineup_path(tuner)?.to_owned(),
                number: channel.path.clone(),
            }),
        })
    }
}

impl LinuxDvbHost {
    pub(crate) async fn open_body(
        &self,
        plan: &DvbStreamPlan,
    ) -> Result<Box<dyn TunerStreamBody>, ServiceError> {
        let lineup = read_lineup(&plan.lineup).await?;
        let channels = parse_lineup(&lineup)?;
        let channel = channels
            .get(plan.number.as_str())
            .ok_or_else(|| ServiceError::not_found(format!("DVB channel {}", plan.number)))?;
        let config = write_tune_config(channel)?;
        let lease = self.acquire_adapter().await?;
        let mut child = Command::new("dvbv5-zap")
            .arg("-c")
            .arg(config.path())
            .arg("-a")
            .arg(lease.adapter.to_string())
            .args(["-s", "-p", "-o", "-", TUNE_SECTION])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(ServiceError::backend_source)?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ServiceError::backend("Missing dvbv5-zap stdout"))?;
        Ok(Box::new(DvbBody {
            child: Some(child),
            stdout,
            lease: Some(lease),
            config: Some(config),
        }))
    }
}

fn write_tune_config(channel: &Channel<'_>) -> Result<tempfile::NamedTempFile, ServiceError> {
    use std::io::Write as _;

    let mut config = tempfile::NamedTempFile::new().map_err(ServiceError::backend_source)?;
    writeln!(config, "[{TUNE_SECTION}]\nSERVICE_ID = {}", channel.service)
        .map_err(ServiceError::backend_source)?;
    for line in channel.section.lines().skip(1) {
        if line
            .split_once('=')
            .is_none_or(|(key, _)| key.trim() != "SERVICE_ID")
        {
            writeln!(config, "{line}").map_err(ServiceError::backend_source)?;
        }
    }
    Ok(config)
}

impl LinuxDvbHost {
    async fn acquire_adapter(&self) -> Result<AdapterLease, ServiceError> {
        let mut adapters = tokio::fs::read_dir(DEVICE_ROOT)
            .await
            .map_err(ServiceError::backend_source)?;
        let mut available = BTreeSet::new();
        while let Some(entry) = adapters
            .next_entry()
            .await
            .map_err(ServiceError::backend_source)?
        {
            let name = entry.file_name();
            let Some(adapter) = name
                .to_str()
                .and_then(|name| name.strip_prefix("adapter"))
                .and_then(|n| n.parse().ok())
            else {
                continue;
            };
            let path = entry.path();
            if tokio::fs::try_exists(path.join("frontend0"))
                .await
                .unwrap_or(false)
                && tokio::fs::try_exists(path.join("demux0"))
                    .await
                    .unwrap_or(false)
                && tokio::fs::try_exists(path.join("dvr0"))
                    .await
                    .unwrap_or(false)
            {
                available.insert(adapter);
            }
        }
        let mut in_use = self
            .in_use
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let adapter = available
            .difference(&in_use)
            .next()
            .copied()
            .ok_or_else(|| ServiceError::Conflict("No free Linux DVB adapter".to_owned()))?;
        in_use.insert(adapter);
        Ok(AdapterLease {
            adapter,
            in_use: Arc::clone(&self.in_use),
        })
    }
}

struct AdapterLease {
    adapter: u32,
    in_use: Arc<Mutex<BTreeSet<u32>>>,
}

impl Drop for AdapterLease {
    fn drop(&mut self) {
        self.in_use
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.adapter);
    }
}

struct DvbBody {
    child: Option<Child>,
    stdout: ChildStdout,
    lease: Option<AdapterLease>,
    config: Option<tempfile::NamedTempFile>,
}

#[async_trait]
impl TunerStreamBody for DvbBody {
    async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, ServiceError> {
        let mut data = vec![0; 64 * 1024];
        let count = tokio::time::timeout(Duration::from_secs(30), self.stdout.read(&mut data))
            .await
            .map_err(|_| ServiceError::backend("Linux DVB capture timed out"))?
            .map_err(ServiceError::backend_source)?;
        data.truncate(count);
        Ok((count != 0).then_some(data))
    }
}

impl Drop for DvbBody {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let lease = self.lease.take();
            let config = self.config.take();
            let _ = child.start_kill();
            tokio::spawn(async move {
                let _ = child.wait().await;
                drop((lease, config));
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_service_config_with_duplicate_station_names() {
        let input = "[WABC]\nVCHANNEL=7.1\nFREQUENCY=123\nSERVICE_ID=1\nVIDEO_PID=49\n\n[WABC]\nVCHANNEL=7.2\nFREQUENCY=123\nSERVICE_ID=0x02\nVIDEO_PID=65\n";
        let channels = parse_lineup(input).unwrap();
        let config = write_tune_config(&channels["7.2"]).unwrap();
        let content = std::fs::read_to_string(config.path()).unwrap();
        assert_eq!(
            content,
            "[ferrofin]\nSERVICE_ID = 2\nVCHANNEL=7.2\nFREQUENCY=123\nVIDEO_PID=65\n"
        );
        assert!(parse_lineup(&format!("{input}{input}")).is_err());
    }
}
