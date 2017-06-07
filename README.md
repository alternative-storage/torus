# Torus
[![Build Status](https://travis-ci.org/alternative-storage/torus.svg?branch=master)](https://travis-ci.org/alternative-storage/torus)
[![Go Report Card](https://goreportcard.com/badge/github.com/alternative-storage/torus)](https://goreportcard.com/report/github.com/alternative-storage/torus)
[![GoDoc](https://godoc.org/github.com/alternative-storage/torus?status.svg)](https://godoc.org/github.com/alternative-storage/torus)

## Torus Overview

Torus is an open source project for distributed storage coordinated through [etcd](https://github.com/coreos/etcd).

Torus provides a resource pool and basic file primitives from a set of daemons running atop multiple nodes. These primitives are made consistent by being append-only and coordinated by [etcd](https://github.com/coreos/etcd). From these primitives, a Torus server can support multiple types of volumes, the semantics of which can be broken into subprojects. It ships with a simple block-device volume plugin, but is extensible to more.

![Quick-glance overview](Documentation/torus-overview.png)

Sharding is done via a consistent hash function, controlled in the simple case by a hash ring algorithm, but fully extensible to arbitrary maps, rack-awareness, and other nice features. The project name comes from this: a hash 'ring' plus a 'volume' is a torus. 

## Project Status and Background

This project is experimental status at this point.

After forked from [original project](https://github.com/coreos/torus)(retired currently), this project integrated [opentracing](http://opentracing.io/) with [Jaeger](http://jaeger.readthedocs.io/).

![opentracing screen](Documentation/opentracing-span.png)

It plans to create a bit more new feature, but performance improvment is the highest priority.

## Trying out Torus

To get started quicky using Torus for the first time, start with the guide to [running your first Torus cluster](Documentation/getting-started.md), learn more about setting up Torus on Kubernetes using FlexVolumes [in contrib](contrib/kubernetes).

## Contributing to Torus

Torus is an open source project and contributors are welcome!

## Licensing

Unless otherwise noted, all code in the Torus repository is licensed under the [Apache 2.0 license](LICENSE). Some portions of the codebase are derived from other projects under different licenses; the appropriate information can be found in the header of those source files, as applicable.
