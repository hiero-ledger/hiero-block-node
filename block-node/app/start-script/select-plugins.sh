if [ -n "${BN_PLUGINS:-}" ]; then
  available=$(ls "$APP_HOME/plugin-catalog" 2>/dev/null | paste -sd, -)
  [ "$BN_PLUGINS" = "all" ] && BN_PLUGINS=$available
  selected=$(printf '%s' "$BN_PLUGINS" | tr ',' ' ')
  for p in $selected; do
    [ -d "$APP_HOME/plugin-catalog/$p" ] || die "ERROR: unknown plugin '$p' in BN_PLUGINS; available: $available"
  done
  manifest=$APP_HOME/plugins/.catalog-jars
  [ -f "$manifest" ] && (cd "$APP_HOME/plugins" && xargs rm -f < "$manifest")
  : > "$manifest"
  for p in $selected; do
    cp "$APP_HOME/plugin-catalog/$p/"*.jar "$APP_HOME/plugins/" || die "ERROR: could not copy plugin '$p' into $APP_HOME/plugins"
    ls "$APP_HOME/plugin-catalog/$p" >> "$manifest"
  done
fi
MODULE_PATH=$APP_HOME/lib/:$APP_HOME/plugins/
