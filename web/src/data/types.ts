export interface RiskLevel {
  id: string;
  label_fr: string;
  min: number;
  max: number;
  alert: boolean;
}

export interface Meta {
  schema_version: number;
  is_sample: boolean;
  sample_notice: string | null;
  generated_at: string;
  window_start: string;
  window_end: string;
  latest_fetched_at: string;
  gdelt_batches: number;
  events_total: number;
  score_version: string;
  risk_levels: RiskLevel[];
  portfolio_label: string;
}

export interface SupplierScore {
  supplier_id: string;
  name: string;
  category: string;
  category_label: string;
  city: string;
  country: string;
  iso2: string;
  region: string;
  lat: number;
  lon: number;
  sole_source: boolean;
  purchase_share_pct: number;
  label: string;
  risk_score: number;
  risk_level: string;
  score_local: number;
  score_route: number;
  events_local: number;
  events_route: number;
  top_driver: string | null;
}

export interface ChokepointScore {
  chokepoint_id: string;
  name_fr: string;
  name_en: string;
  lat: number;
  lon: number;
  radius_km: number;
  risk_score: number | null;
  risk_level: string;
  events_recent: number;
  suppliers_exposed: number;
  purchase_share_exposed_pct: number;
  sole_source_exposed: number;
}

export interface SupplierRoute {
  supplier_id: string;
  position: number;
  chokepoint_id: string;
}

export interface SupplierEvent {
  supplier_id: string;
  link_type: "city" | "chokepoint";
  chokepoint_id: string | null;
  distance_km: number;
  global_event_id: number;
  event_date: string;
  fetched_at: string;
  action_geo_name: string | null;
  action_geo_type: number;
  lat: number;
  lon: number;
  event_code: string | null;
  event_root_code: string | null;
  quad_class: number | null;
  goldstein_scale: number | null;
  avg_tone: number | null;
  num_mentions: number | null;
  num_sources: number | null;
  num_articles: number | null;
  source_url: string | null;
  src_file_url: string;
  contribution: number | null;
  llm_category: string | null;
  llm_summary: string | null;
}

export interface Dataset {
  meta: Meta;
  suppliers: SupplierScore[];
  chokepoints: ChokepointScore[];
  routes: SupplierRoute[];
  /** Événements d'un fournisseur (requête DuckDB à la demande). */
  eventsFor(supplierIds: string[]): Promise<Map<string, SupplierEvent[]>>;
}
