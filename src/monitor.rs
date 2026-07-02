use nvml_wrapper::{Nvml, enum_wrappers::device::TemperatureSensor as TempSensor};
use serde_json::{Value, json};
use sysinfo::{Components, System};

use crate::parser::{CpuTicks, parse_proc_stat};

struct GpuData {
    name: String,
    uuid: String,
    bus_id: String,
    temp: i64,
    vram_temp: i64,
    gpu_util: i64,
    mem_util: i64,
    total_mem: i64,
}

pub struct Monitor {
    sys: System,
    components: Components,
    nvml: Option<Nvml>,
    cpu_count: usize,
    phys_cpu_count: usize,
    prev_ticks: Option<CpuTicks>,
}

impl Monitor {
    pub fn new() -> Self {
        let mut sys = System::new();
        sys.refresh_cpu_usage();
        let cpu_count = sys.cpus().len();
        let phys_cpu_count = System::physical_core_count().unwrap_or(0);
        Self {
            sys,
            components: Components::new_with_refreshed_list(),
            nvml: Nvml::init().ok(),
            cpu_count,
            phys_cpu_count,
            prev_ticks: None,
        }
    }

    pub fn collect(&mut self) -> Value {
        let ticks = std::fs::read_to_string("/proc/stat")
            .ok()
            .and_then(|s| parse_proc_stat(&s));

        let util_cpu = match (&self.prev_ticks, &ticks) {
            (Some(prev), Some(curr)) => curr.usage_pct(prev),
            _ => -1,
        };
        self.prev_ticks = ticks;

        self.sys.refresh_memory();
        self.components.refresh(true);

        json!({
            "cpu":  self.collect_cpu(util_cpu),
            "gpu":  self.collect_gpu(),
            "mem":  self.collect_mem(),
            "nvme": [],
            "nic":  { "temp": -1_i64 },
        })
    }

    fn collect_cpu(&self, util_cpu: i64) -> Value {
        json!({
            "num_cores":      self.cpu_count as i64,
            "util_cpu":       util_cpu,
            "temp":           read_cpu_temp(&self.components),
            "physical_cores": self.phys_cpu_count as i64,
        })
    }

    fn collect_mem(&self) -> Value {
        json!({
            "total":     self.sys.total_memory()     as i64,
            "used":      self.sys.used_memory()      as i64,
            "free":      self.sys.free_memory()      as i64,
            "available": self.sys.available_memory() as i64,
        })
    }

    fn collect_gpu(&self) -> Value {
        let nvml = match &self.nvml {
            Some(n) => n,
            None => return gpu_stub(),
        };

        let count = match nvml.device_count() {
            Ok(c) if c > 0 => c,
            _ => return gpu_stub(),
        };

        let gpus: Vec<GpuData> = (0..count)
            .filter_map(|i| {
                let dev = nvml.device_by_index(i).ok()?;
                let util = dev.utilization_rates().ok()?;
                let mem = dev.memory_info().ok()?;
                let pci = dev.pci_info().ok()?;
                Some(GpuData {
                    name: dev.name().ok()?,
                    uuid: dev.uuid().ok()?,
                    bus_id: pci.bus_id,
                    temp: dev.temperature(TempSensor::Gpu).ok()? as i64,
                    vram_temp: -1,
                    gpu_util: util.gpu as i64,
                    mem_util: util.memory as i64,
                    total_mem: mem.total as i64,
                })
            })
            .collect();

        if gpus.is_empty() {
            return gpu_stub();
        }

        let n = gpus.len() as f64;
        let (max_temp, max_vram_temp, sum_gpu, sum_mem, sum_total) = gpus.iter().fold(
            (-1i64, -1i64, 0f64, 0f64, 0f64),
            |(mt, mvt, sg, sm, st), g| {
                (
                    mt.max(g.temp),
                    mvt.max(g.vram_temp),
                    sg + g.gpu_util as f64,
                    sm + g.mem_util as f64,
                    st + g.total_mem as f64,
                )
            },
        );

        let status: serde_json::Map<String, Value> = gpus
            .iter()
            .map(|g| {
                (
                    g.uuid.clone(),
                    json!({
                        "name":      g.name,
                        "bus_id":    g.bus_id,
                        "temp":      g.temp,
                        "vram_temp": g.vram_temp,
                        "util_gpu":  g.gpu_util,
                        "util_mem":  g.mem_util,
                        "total_mem": g.total_mem,
                    }),
                )
            })
            .collect();

        json!({
            "num_online":      gpus.len() as i64,
            "max_gpu_temp":    max_temp,
            "max_vram_temp":   max_vram_temp,
            "avg_util_gpu":    sum_gpu  / n,
            "avg_util_mem":    sum_mem  / n,
            "avg_mem_per_gpu": sum_total / n,
            "status":          status,
        })
    }
}

impl Default for Monitor {
    fn default() -> Self {
        Self::new()
    }
}

fn read_cpu_temp(components: &Components) -> f64 {
    components
        .iter()
        .find(|c| {
            let l = c.label().to_ascii_lowercase();
            l.contains("tdie") || l.contains("package id 0")
        })
        .or_else(|| {
            components.iter().find(|c| {
                let l = c.label().to_ascii_lowercase();
                l.contains("cpu") || l.starts_with("core")
            })
        })
        .and_then(|c| c.temperature())
        .map(|c| c as f64)
        .unwrap_or(-1.0)
}

fn gpu_stub() -> Value {
    json!({
        "num_online":      -1_i64,
        "max_gpu_temp":    -1_i64,
        "max_vram_temp":   -1_i64,
        "avg_util_gpu":    -1.0_f64,
        "avg_util_mem":    -1.0_f64,
        "avg_mem_per_gpu": -1.0_f64,
        "status": {},
    })
}
