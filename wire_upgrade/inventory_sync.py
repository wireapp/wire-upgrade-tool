"""Inventory sync utilities."""

from __future__ import annotations

from dataclasses import dataclass
import re
from pathlib import Path
from typing import Dict, List, Tuple

from wire_upgrade import wire_sync_lib


REQUIRED_HOSTS = [
    "postgresql1",
    "postgresql2",
    "postgresql3",
]

POSTGRESQL_SECTIONS = [
    "postgresql:vars",
    "postgresql",
    "postgresql_rw",
    "postgresql_ro",
]

KUBERNETES_REQUIRED_SECTIONS = [
    "kube-master",
    "kube-node",
    "etcd",
    "k8s-cluster:children",
]

KUBENODE_HOSTS = ["kubenode1", "kubenode2", "kubenode3"]


@dataclass
class HostEntry:
    host: str
    vars: Dict[str, str]


def _strip_generated_header(lines: List[str]) -> List[str]:
    cleaned = []
    for line in lines:
        if line.startswith("# Generated from old inventory"):
            continue
        if line.startswith("# Template base:"):
            continue
        if line.startswith("# Source:"):
            continue
        cleaned.append(line)
    return cleaned


def parse_template(template_path: Path) -> Tuple[List[str], List[str], Dict[str, List[str]]]:
    header_lines: List[str] = []
    section_lines: Dict[str, List[str]] = {}
    section_order: List[str] = []
    current = None

    for raw in template_path.read_text().splitlines():
        line = raw.strip()
        if line.startswith("[") and line.endswith("]"):
            current = line[1:-1]
            section_order.append(current)
            section_lines.setdefault(current, [])
            continue
        if current is None:
            header_lines.append(raw)
        else:
            section_lines[current].append(raw)

    header_lines = _strip_generated_header(header_lines)
    return section_order, header_lines, section_lines


def extract_section_hosts(lines: List[str]) -> List[str]:
    hosts = []
    for raw in lines:
        stripped = raw.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            stripped = stripped.lstrip("#").strip()
        if not stripped:
            continue
        tokens = stripped.split()
        token = tokens[0]
        if not re.match(r"^[a-z][a-z0-9_-]*$", token):
            continue
        if "=" in token:
            continue
        if len(tokens) > 1 and not any("=" in t for t in tokens[1:]):
            continue
        if re.match(r"^\d{1,3}(?:\.\d{1,3}){3}$", token):
            continue
        hosts.append(token)
    return hosts


def prompt_hosts(hosts: List[str], defaults: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    results: Dict[str, Dict[str, str]] = {}

    if not hosts:
        return results

    ordered = [h for h in REQUIRED_HOSTS if h in hosts]
    default_ips = [defaults.get(h, {}).get("ansible_host", "") for h in ordered]
    default_hint = ", ".join([ip for ip in default_ips if ip])

    prompt = (
        "Provide IPs in order ({}), comma-separated".format(
            ", ".join(ordered)
        )
    )
    if default_hint and len(default_ips) == len(ordered):
        prompt += f" [default: {', '.join(default_ips)}]"
    prompt += ". Leave blank to enter individually: "

    ips_line = input(prompt).strip()
    if not ips_line and default_hint and len(default_ips) == len(ordered):
        ips_line = ",".join(default_ips)

    if ips_line:
        ips = [v.strip() for v in ips_line.split(",") if v.strip()]
        if len(ips) == len(ordered):
            for host, ip in zip(ordered, ips):
                results[host] = {"ansible_host": ip, "ip": ip}

    for host in ordered:
        if host in results:
            continue
        default_ip = defaults.get(host, {}).get("ansible_host", "")
        prompt_ip = f"IP for {host}"
        if default_ip:
            prompt_ip += f" [default {default_ip}]"
        prompt_ip += ": "
        ip = input(prompt_ip).strip() or default_ip
        if not ip:
            continue
        results[host] = {"ansible_host": ip, "ip": ip}

    return results


def format_host_comment(entry: HostEntry) -> str:
    vars_map = dict(entry.vars)
    ordered = []
    ansible_host = vars_map.pop("ansible_host", "")
    ip = vars_map.pop("ip", "")
    if ansible_host:
        ordered.append("ansible_host=" + ansible_host)
    if not ip and ansible_host:
        ip = ansible_host
    if ip:
        ordered.append("ip=" + ip)
    for key in sorted(vars_map.keys()):
        ordered.append(f"{key}={vars_map[key]}")
    if ordered:
        return "# " + " ".join([entry.host] + ordered)
    return "# " + entry.host


def build_hosts_ini(
    section_order: List[str],
    header_lines: List[str],
    template_sections: Dict[str, List[str]],
    all_hosts: List[HostEntry],
    all_vars: List[str],
    groups: Dict[str, List[str]],
    output_path: Path,
) -> None:
    host_names = {h.host for h in all_hosts}
    lines: List[str] = []
    lines.append("# Generated from 99-static")
    lines.append(f"# Template: {output_path.parent / '99-static'}")
    lines.append("")
    lines.extend(header_lines)

    def emit_section(name: str, body: List[str]):
        lines.append(f"[{name}]")
        if body:
            lines.extend(body)
        lines.append("")

    all_lines = []
    for entry in all_hosts:
        vars_map = dict(entry.vars)
        ansible_host = vars_map.pop("ansible_host", "")
        ip = vars_map.pop("ip", "") or ansible_host
        ordered = []
        if ansible_host:
            ordered.append("ansible_host=" + ansible_host)
        if ip:
            ordered.append("ip=" + ip)
        for key in sorted(vars_map.keys()):
            ordered.append(f"{key}={vars_map[key]}")
        all_lines.append(" ".join([entry.host] + ordered))

    postgres_hosts = [
        h.host for h in all_hosts
        if h.host in {"postgresql1", "postgresql2", "postgresql3"}
    ]

    def postgres_body(section: str) -> List[str]:
        if section == "postgresql":
            return postgres_hosts
        if section == "postgresql_rw":
            return ["postgresql1"] if "postgresql1" in postgres_hosts else []
        if section == "postgresql_ro":
            return [h for h in postgres_hosts if h != "postgresql1"]
        return template_sections.get(section, [])

    for section in section_order:
        if section == "all":
            emit_section(section, all_lines)
            continue
        if section == "all:vars":
            emit_section(section, all_vars)
            continue
        if section.endswith(":vars"):
            if section in POSTGRESQL_SECTIONS:
                emit_section(section, template_sections.get(section, []))
            else:
                emit_section(section, groups.get(section, template_sections.get(section, [])))
            continue
        if section.endswith(":children"):
            emit_section(section, groups.get(section, template_sections.get(section, [])))
            continue

        if section in POSTGRESQL_SECTIONS:
            emit_section(section, postgres_body(section))
            continue

        body = groups.get(section)
        if body is None or not body:
            template_hosts = extract_section_hosts(template_sections.get(section, []))
            body = [h for h in template_hosts if h in host_names]
        emit_section(section, body)

    output_path.write_text("\n".join(lines).rstrip() + "\n")


def sync_inventory(old_inventory: Path, new_bundle: Path) -> Tuple[Path, Path]:
    template_path = new_bundle / "ansible" / "inventory" / "offline" / "99-static"
    new_inventory = new_bundle / "ansible" / "inventory" / "offline" / "hosts.ini"

    section_order, header_lines, template_sections = parse_template(template_path)
    try:
        all_hosts_raw, all_vars, groups = wire_sync_lib.parse_hosts_ini(old_inventory)
    except FileNotFoundError:
        raise FileNotFoundError(f"Missing source hosts.ini: {old_inventory}")

    all_hosts: List[HostEntry] = []
    for entry in all_hosts_raw:
        all_hosts.append(HostEntry(entry["host"], dict(entry["vars"])))

    template_all_hosts = extract_section_hosts(template_sections.get("all", []))
    template_defaults: Dict[str, Dict[str, str]] = {}
    for raw in template_sections.get("all", []):
        stripped = raw.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            stripped = stripped.lstrip("#").strip()
        if not stripped:
            continue
        tokens = stripped.split()
        host = tokens[0]
        if not re.match(r"^[a-z][a-z0-9_-]*$", host):
            continue
        if re.match(r"^\d{1,3}(?:\.\d{1,3}){3}$", host):
            continue
        vars_map: Dict[str, str] = {}
        for token in tokens[1:]:
            if "=" in token:
                k, v = token.split("=", 1)
                if "REPLACE_WITH" in v or v.upper() == "XXXX":
                    continue
                vars_map[k] = v
        if vars_map:
            template_defaults[host] = vars_map
    existing = {h.host for h in all_hosts}

    missing = []
    for host in REQUIRED_HOSTS:
        if host in template_all_hosts and host not in existing and host not in missing:
            missing.append(host)

    ansnode_ips = [
        entry.vars.get("ansible_host", "")
        for entry in all_hosts
        if entry.host.startswith("ansnode") and entry.vars.get("ansible_host")
    ]
    defaults: Dict[str, Dict[str, str]] = {}
    if ansnode_ips:
        for idx, host in enumerate(missing):
            ip = ansnode_ips[idx % len(ansnode_ips)]
            defaults[host] = {"ansible_host": ip, "ip": ip}

    if missing:
        merged_defaults = dict(template_defaults)
        for host, vars_map in defaults.items():
            merged_defaults.setdefault(host, vars_map)
        if all(host in merged_defaults for host in missing):
            added = {host: merged_defaults[host] for host in missing}
        else:
            added = prompt_hosts(missing, merged_defaults)
        for host, vars_map in added.items():
            all_hosts.append(HostEntry(host, vars_map))

    missing_pg_sections = [s for s in POSTGRESQL_SECTIONS if s not in template_sections]
    if missing_pg_sections:
        print("Warnings:")
        for section in missing_pg_sections:
            print(f"  - Missing PostgreSQL section in template: [{section}]")

    derived_groups: Dict[str, List[str]] = {k: list(v) for k, v in groups.items()}

    build_hosts_ini(
        section_order,
        header_lines,
        template_sections,
        all_hosts,
        all_vars,
        derived_groups,
        new_inventory,
    )

    return template_path, new_inventory


def validate_inventory(inventory_path: Path) -> Tuple[List[str], List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []
    passed: List[str] = []

    try:
        all_hosts_raw, all_vars, groups = wire_sync_lib.parse_hosts_ini(inventory_path)
    except FileNotFoundError:
        return [f"Missing inventory: {inventory_path}"], [], []

    all_hosts = {entry["host"]: entry["vars"] for entry in all_hosts_raw}

    for host in KUBENODE_HOSTS:
        if host not in all_hosts:
            errors.append(f"Missing {host} in [all]")
        else:
            if "ansible_host" not in all_hosts[host]:
                errors.append(f"Missing ansible_host for {host} in [all]")
    if not any(e.startswith("Missing kubenode") or "ansible_host for kubenode" in e for e in errors):
        passed.append("[all]")

    for section in KUBERNETES_REQUIRED_SECTIONS:
        if section not in groups:
            errors.append(f"Missing section: [{section}]")
    if all(section in groups for section in KUBERNETES_REQUIRED_SECTIONS):
        passed.extend([f"[{s}]" for s in KUBERNETES_REQUIRED_SECTIONS])

    kube_master = set(groups.get("kube-master", []))
    kube_node = set(groups.get("kube-node", []))
    etcd_lines = groups.get("etcd", [])
    etcd_hosts = set()
    for line in etcd_lines:
        parts = line.split()
        if not parts:
            continue
        host = parts[0]
        etcd_hosts.add(host)
        if not any("etcd_member_name=" in p for p in parts[1:]):
            errors.append(f"Missing etcd_member_name for {host} in [etcd]")

    for host in KUBENODE_HOSTS:
        if host not in kube_master:
            errors.append(f"{host} missing from [kube-master]")
        if host not in kube_node:
            errors.append(f"{host} missing from [kube-node]")
        if host not in etcd_hosts:
            errors.append(f"{host} missing from [etcd]")

    k8s_children = set(groups.get("k8s-cluster:children", []))
    if "kube-master" not in k8s_children:
        errors.append("[k8s-cluster:children] missing kube-master")
    if "kube-node" not in k8s_children:
        errors.append("[k8s-cluster:children] missing kube-node")

    for section in ["cassandra", "cassandra_seed", "elasticsearch", "elasticsearch_master:children", "minio", "rmq-cluster", "postgresql", "postgresql_rw", "postgresql_ro"]:
        if section in groups:
            passed.append(f"[{section}]")
        else:
            errors.append(f"Missing section: [{section}]")

    def hosts_exist(section: str):
        for line in groups.get(section, []):
            parts = line.split()
            if not parts:
                continue
            host = parts[0]
            if host not in all_hosts:
                errors.append(f"{host} referenced in [{section}] missing from [all]")

    for section in ["cassandra", "elasticsearch", "minio", "rmq-cluster", "postgresql", "postgresql_rw", "postgresql_ro"]:
        if section in groups:
            hosts_exist(section)

    if not all_vars:
        warnings.append("No [all:vars] entries found")

    return errors, warnings, sorted(set(passed))
