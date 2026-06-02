FROM gcr.io/distroless/static-debian12:nonroot
COPY tracearr /tracearr
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/tracearr"]
CMD ["-config", "/etc/tracearr/config.yaml"]
