# health

Exposes the standard HTTP health check endpoints used by Kubernetes and load balancers to determine whether the Block Node is alive and ready to serve traffic.

---

## Key Files

|            File            |                                                                            Purpose                                                                             |
|----------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `HealthServicePlugin.java` | The entire plugin. Registers `/livez` and `/readyz` HTTP routes and returns `200 OK` or `503 Service Unavailable` based on the current `HealthFacility` state. |

---

## Notable Logic

### Liveness vs. readiness — both use the same check

Both `/livez` and `/readyz` currently call `healthFacility.isRunning()`. In Kubernetes, liveness and readiness are semantically different: liveness answers "is the process alive?" while readiness answers "is it ready to receive traffic?". A node that is still initialising storage plugins should return `503` on `/readyz` but `200` on `/livez`. Today both endpoints return the same result, which means Kubernetes may route traffic to a node that is not yet ready to serve. A future improvement is to make `/readyz` check that all `BlockProviderPlugin` instances have completed their `start()` phase.
