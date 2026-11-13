# Linux DVB tuner

Ferrofin can use local Linux DVB adapters through `dvbv5-zap` from v4l-utils.
It copies the tuner's MPEG transport stream into the shared Live TV buffer.
Viewers and DVR recordings share that buffer. Ferrofin can remux or transcode
the stream for playback using its existing FFmpeg pipeline.

Install v4l-utils and grant the Ferrofin service user access to the frontend,
demux, and DVR devices under `/dev/dvb`. Give Ferrofin exclusive use of the
adapters during testing. It leases the first free adapter with `frontend0`,
`demux0`, and `dvr0` and holds the lease until the capture process exits.

Use a readable dvbv5-format lineup, such as ZapLink's ATSC `channels.conf`.
Each station needs a unique `VCHANNEL`, a `FREQUENCY`, a `SERVICE_ID`, and
the delivery-system and PID fields needed by dvbv5-zap. The tuner uses the
virtual channel number to match XMLTV listings. Ferrofin writes only the
selected station into the temporary tuning file so repeated section names
or a shared RF frequency cannot select a different service.

Create a tuner with the authenticated administrator API
`POST /LiveTv/TunerHosts` and this JSON body.

```json
{
  "Type": "linuxdvb",
  "Url": "/etc/ferrofin/channels.conf",
  "FriendlyName": "Local antenna"
}
```

Run **Refresh Guide** to load the channels and match your existing XMLTV
provider. Linux DVB channels have their own IDs. Before replacing an existing
M3U tuner, re-create or migrate recording rules that reference its channel IDs.
Keep the configured lineup path stable to preserve the Linux DVB channel IDs.
