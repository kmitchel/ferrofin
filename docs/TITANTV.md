# Use TitanTV for the Live TV guide

Ferrofin's TitanTV listings provider fetches the lineup and schedule directly
from TitanTV. Configure a Live TV tuner that receives the stations in the lineup
and set the Ferrofin process's local timezone to the lineup's timezone. TitanTV
uses local times without UTC offsets for schedule requests and most airings.

## Add the provider

Find your TitanTV user and lineup UUIDs in the TitanTV website's
Network panel. When you open your lineup, look for a request of the form
`/api/channel/<user UUID>/<lineup UUID>`. Keep these identifiers private.

Use the authenticated administrator API `POST /LiveTv/ListingProviders` to
create the provider. To update it, include its existing `Id` and settings.
The standard Jellyfin web client has no TitanTV setup form.

```json
{
  "Type": "TitanTV",
  "Username": "<TitanTV user UUID>",
  "ListingsId": "<TitanTV lineup UUID>",
  "EnableAllTuners": true
}
```

The save checks that TitanTV returns the lineup and queues a guide refresh.
For selected tuners, set `EnableAllTuners` to `false` and pass their IDs in `EnabledTuners`.
Use a single listings provider per tuner to avoid competing guide updates.

## Match stations and refresh

Open the Live TV guide after **Refresh Guide** finishes. Ferrofin's existing
channel matcher uses the TitanTV station ID, channel number, and name.
Use `GET /LiveTv/ChannelMappingOptions?providerId=<provider ID>` to inspect
the lineup and `POST /LiveTv/ChannelMappings` with `ProviderId`,
`TunerChannelId`, and `ProviderChannelId` to map a station whose IDs or
channel numbers differ.

Ferrofin fetches up to seven days in six-hour blocks. Successful blocks update
the guide during a partial refresh, and previously imported listings are
retained until a complete refresh can reconcile them. For a failed block,
the server journal identifies the requested local time and records the HTTP
status, TitanTV error code, or response problem without logging the account
IDs.
