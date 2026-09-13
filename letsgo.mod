// letsgo.mod

// letsgo's default matrix includes windows/amd64, which this project has never
// shipped, so the four targets the GoReleaser config built are listed instead.
build (
	linux/amd64
	linux/arm64
	darwin/amd64
	darwin/arm64
)

// Replaces the GoReleaser archive file list. Naming any file at all replaces
// letsgo's default of README and LICENSE, so those are repeated here; the paths
// are plain rather than globs, so the two scripts are named individually.
archive (
	README.md
	LICENSE
	config.example.yaml
	scripts/nzbget/tracearr.py
	scripts/sabnzbd/tracearr.py
)

// Replaces the two `dockers` entries and the `docker_manifests` that stitched
// them together; letsgo assembles the multi-arch index itself.
image ghcr.io/danielriddell21/tracearr

// The base the deleted Dockerfile used; it also supplies the nonroot user that
// Dockerfile set by hand. Pinned by digest so two releases of one commit cannot
// differ — bump it deliberately for base fixes.
image base gcr.io/distroless/static-debian12@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab

// Releases were marked as pre-releases by the shared workflow after the fact;
// letsgo does it as part of publishing, so promotion is still a manual step.
release prerelease=true
