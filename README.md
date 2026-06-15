# Agent Smith Lite

`agent smith lite` is a small utility program to ship back local metrics to the control plane for integrated health/system management. 

## Building

Download a pre-built release or checkout the repo and build with `cargo` - `cargo build --release`.

The resulting binary can be found under `target/release` as `agent_smith_lite`.

Please note that currently all pre-built releases are designed for Ubuntu 22.04+ (x86).

## Usage

First you need to create a token for the agent to communicate with control plane .

Go to Organisations > Manage > Agent Tokens.

Next follow the prompts to generate a token - ie give a name `US East DB machine` - and make sure to save the token. Note currently tokens expire after 1 year.

To then connect your machine just run the agent with the following environment variables
```
AS_ACCELERATOR_TYPE="${cuda or cpu}"
AS_TOKEN="${your token previously generated}"
AS_ENDPOINT=wss://strongcontrol.com/agent_smith_socket/websocket
./agent-smith-lite
```
The agent will also create a `agent_smith_lite.uuid` file which will store its assigned id from the control plane. Make sure it can read and write to this file to keep consistent tracking.

## Note

You can have multiple agents running from a single ip (eg vms on a machine) as long as they: 
1. All use different tokens to communicate with the control plane
2. Do not share a common directory (i.e. each binary/id combo must reside in it's own folder)


## Roadmap

Roadmap - along with just health metrics, we intend to release extra data management and cluster configuration tooling as pluggable upgrades.
