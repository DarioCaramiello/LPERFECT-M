# LPERFECT – Model Description
**Lagrangian Parallel Environmental Runoff and Flood Evaluation for Computational Terrain**

---

## 1. Overview

LPERFECT is a **Lagrangian, particle-based hydrological model** designed to estimate surface runoff, flood propagation, and hydrogeological risk over gridded terrains.  
It is optimized for **parallel execution (MPI)** and **operational workflows**, and is fully integrated in the **Hi-WeFAI** (*High-Performance Weather and Flood AI Systems*) project.

Unlike full hydrodynamic solvers, LPERFECT focuses on **computational efficiency, robustness, and scalability**, making it suitable for:
- flood nowcasting,
- ensemble simulations,
- AI-driven rainfall impact assessment,
- HPC and cloud deployments.

---

## 2. Conceptual Architecture

![LPERFECT conceptual pipeline](figures/lperfect_pipeline.png)

**Pipeline steps:**

1. Rainfall ingestion (multi-source, time-aware)
2. Runoff generation (SCS Curve Number)
3. Lagrangian particle spawning
4. D8-based routing with travel-time control
5. Particle migration across MPI domains
6. Flood depth reconstruction
7. Hydrogeological risk assessment

---

## 3. Governing Concepts

### 3.1 Lagrangian Representation

Water is represented by **discrete particles**, each carrying a small volume of water:

- particles are created from incremental runoff,
- particles move along D8 flow directions,
- routing is controlled by **travel time parameters**,
- mass conservation is ensured by construction.

This formulation avoids global CFL constraints and is **naturally parallelizable**.

![Particle routing](figures/particle_routing.png)

---

## 4. Runoff Generation

LPERFECT uses the **SCS Curve Number (CN)** method in its cumulative form.

### Equations

Potential retention:

\[
S = \frac{25400}{CN} - 254
\]

Initial abstraction:

\[
I_a = \alpha S
\]

Cumulative runoff:

\[
Q =
\begin{cases}
0 & P \le I_a \\
\frac{(P - I_a)^2}{P - I_a + S} & P > I_a
\end{cases}
\]

Where:
- \(P\) is cumulative precipitation (mm),
- \(Q\) is cumulative runoff (mm),
- \(\alpha\) is the initial abstraction ratio.

Incremental runoff per timestep is:

\[
\Delta Q = Q(t) - Q(t-1)
\]

---

## 5. Lagrangian Routing

### 5.1 D8 Flow Network

Each grid cell routes water to **one downstream neighbor** defined by a D8 direction grid.

Supported encodings:
- ESRI (1,2,4,8,16,32,64,128)
- Clockwise (0–7)

### 5.2 Travel Time

Particles advance only when their internal timer \(\tau \le 0\).  
After each hop:

- hillslope cell: \(\tau = t_{hill}\)
- channel cell: \(\tau = t_{channel}\)

This allows:
- sub-timestep control,
- channel acceleration,
- numerical stability.

---

## 6. Parallelization Strategy

![MPI slab decomposition](figures/mpi_slabs.png)

LPERFECT uses **row-slab domain decomposition**:

- each MPI rank owns a contiguous block of rows,
- particles belong to the rank owning their current row,
- particles crossing slab boundaries are migrated using `MPI_Alltoallv`.

**Key properties:**
- no global synchronization during routing,
- communication proportional to boundary crossings,
- excellent weak scalability.

---

## 7. Restartable State Model

The full model state is checkpointed in **NetCDF**:

Stored quantities:
- cumulative precipitation and runoff fields,
- particle positions, volumes, and timers,
- elapsed simulation time,
- cumulative mass diagnostics.

![Restart workflow](figures/restart_workflow.png)

This enables:
- fault tolerance,
- operational chaining,
- ensemble splitting.

---

## 8. Flood Depth Reconstruction

Flood depth is reconstructed by aggregating particle volumes:

\[
h(i,j) = \frac{\sum V_p(i,j)}{A(i,j)}
\]

Where:
- \(V_p\) is particle volume,
- \(A\) is cell area.

The result is a **spatially distributed flood depth map**.

---

## 9. Hydrogeological Risk Index

LPERFECT computes a **dimensionless risk index** combining:

1. Direct hydrological forcing (runoff),
2. Morphological control (flow accumulation).

### Definition

\[
R = \beta \, \hat{Q} + (1 - \beta) \, \hat{A}
\]

Where:
- \(\hat{Q}\) = normalized cumulative runoff,
- \(\hat{A}\) = normalized flow accumulation,
- \(\beta\) = balance parameter.

Normalization is performed using robust percentiles.

![Risk index concept](figures/risk_index.png)

---

## 10. Input and Output Data Model

### Input
- Domain NetCDF (DEM, D8, CN, optional channel mask)
- Rainfall NetCDFs (time-aware)

### Output
- `flood_depth(y,x)` [m]
- `risk_index(y,x)` [-]

All outputs are **CF-1.10 compliant**.

---

## 11. Example Workflow

```bash
mpirun -np 8 python main.py --config config.json
```

```python
import xarray as xr
ds = xr.open_dataset("flood_depth.nc")
ds.flood_depth.plot()
```

---

## 12. Scope and Limitations

LPERFECT is:
- ✔ fast and scalable,
- ✔ mass conservative,
- ✔ well suited for ensembles and nowcasting.

LPERFECT is **not**:
- a 2D shallow-water solver,
- a replacement for detailed hydraulic models.

---

## 13. Outlook

Planned extensions include:
- distributed NetCDF I/O,
- GPU acceleration,
- alternative infiltration models,
- coupling with hydraulic solvers,
- uncertainty propagation.

---

**LPERFECT** – a computationally efficient bridge between rainfall intelligence and flood-risk awareness.
