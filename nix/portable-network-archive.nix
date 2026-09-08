{
  stdenv,
  fetchurl,
  autoPatchelfHook,
  acl,
  lib,
  system,
}:

let
  version = "0.38.1";
  sources = {
    x86_64-linux = {
      target = "x86_64-unknown-linux-gnu";
      hash = "sha256-sc8JLwrzEWbihZ3MBeM65hZAAdME+ImumuuCaPRCeno=";
    };
    aarch64-linux = {
      target = "aarch64-unknown-linux-gnu";
      hash = "sha256-S1uGqb3wRn5YkPwuFROSNsmeYgNQAEPL0vgwFUnlrns=";
    };
  };
  source = sources.${system} or (throw "portable-network-archive: unsupported system ${system}");
in
stdenv.mkDerivation {
  pname = "portable-network-archive";
  inherit version;

  src = fetchurl {
    url = "https://github.com/ChanTsune/Portable-Network-Archive/releases/download/portable-network-archive-${version}/portable-network-archive-${source.target}.tar.xz";
    inherit (source) hash;
  };

  nativeBuildInputs = [ autoPatchelfHook ];
  buildInputs = [ acl stdenv.cc.cc.lib ];

  installPhase = ''
    runHook preInstall

    pna_bin="$(find . -type f -name pna -print -quit)"
    if [ -z "$pna_bin" ]; then
      echo "pna binary not found in release archive" >&2
      exit 1
    fi

    install -Dm755 "$pna_bin" "$out/bin/pna"

    runHook postInstall
  '';

  meta = {
    description = "Portable Network Archive command-line utility";
    homepage = "https://github.com/ChanTsune/Portable-Network-Archive";
    license = with lib.licenses; [ mit asl20 ];
    mainProgram = "pna";
    platforms = builtins.attrNames sources;
    sourceProvenance = with lib.sourceTypes; [ binaryNativeCode ];
  };
}
