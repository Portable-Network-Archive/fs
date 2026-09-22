FROM nixos/nix:2.28.5 AS dev

ENV NIX_CONFIG="experimental-features = nix-command flakes"
ENV CARGO_TARGET_DIR=/tmp/target/
ENV PKG_CONFIG_PATH="/root/.nix-profile/lib/pkgconfig:/root/.nix-profile/share/pkgconfig"
ENV PATH="/root/.cargo/bin:/root/.nix-profile/bin:${PATH}"

WORKDIR /workspace

COPY flake.nix ./
COPY nix ./nix
COPY scripts/update-pna-package.sh ./scripts/update-pna-package.sh

RUN nix profile install --no-write-lock-file .#dev-tools \
    && rustc --version \
    && cargo --version \
    && pna --version \
    && pkg-config --version \
    && fusermount3 --version
