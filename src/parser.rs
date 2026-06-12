use nom::{
    IResult,
    bytes::complete::{tag, take_till},
    character::complete::{digit1, line_ending, space1},
    combinator::{map_res, opt},
};

#[derive(Debug, Clone)]
pub struct CpuTicks {
    pub user: u64,
    pub nice: u64,
    pub system: u64,
    pub idle: u64,
    pub iowait: u64,
}

impl CpuTicks {
    pub fn usage_pct(&self, prev: &Self) -> i64 {
        let total = (self.user + self.nice + self.system + self.idle + self.iowait)
            .saturating_sub(prev.user + prev.nice + prev.system + prev.idle + prev.iowait);
        if total == 0 {
            return -1;
        }
        let busy = (self.user + self.nice + self.system)
            .saturating_sub(prev.user + prev.nice + prev.system);
        ((busy as f64 / total as f64) * 100.0).round() as i64
    }
}

fn u64_val(input: &str) -> IResult<&str, u64> {
    map_res(digit1, |s: &str| s.parse::<u64>())(input)
}

fn cpu_ticks_line(input: &str) -> IResult<&str, CpuTicks> {
    let (i, _) = tag("cpu")(input)?;
    let (i, _) = space1(i)?;
    let (i, user) = u64_val(i)?;
    let (i, _) = space1(i)?;
    let (i, nice) = u64_val(i)?;
    let (i, _) = space1(i)?;
    let (i, system) = u64_val(i)?;
    let (i, _) = space1(i)?;
    let (i, idle) = u64_val(i)?;
    let (i, _) = space1(i)?;
    let (i, iowait) = u64_val(i)?;
    let (i, _) = take_till(|c| c == '\n')(i)?;
    let (i, _) = opt(line_ending)(i)?;
    Ok((
        i,
        CpuTicks {
            user,
            nice,
            system,
            idle,
            iowait,
        },
    ))
}

pub fn parse_proc_stat(input: &str) -> Option<CpuTicks> {
    cpu_ticks_line(input).ok().map(|(_, t)| t)
}
