MODULE_PATH=$APP_HOME/lib/
available=$(ls "$APP_HOME/plugin-catalog" 2>/dev/null | paste -sd, -)
if [ "${BN_PLUGINS:-}" = "all" ]; then
  BN_PLUGINS=$available
fi
for p in $(printf '%s' "${BN_PLUGINS:-}" | tr ',' ' '); do
  [ -d "$APP_HOME/plugin-catalog/$p" ] || die "ERROR: unknown plugin '$p' in BN_PLUGINS; available: $available"
  MODULE_PATH=$MODULE_PATH:$APP_HOME/plugin-catalog/$p/
done
MODULE_PATH=$MODULE_PATH:$APP_HOME/plugins/
