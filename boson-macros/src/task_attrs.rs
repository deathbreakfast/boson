use syn::parse::Parse;
use syn::parse::ParseStream;
use syn::{Ident, LitFloat, LitInt, LitStr, Token};

pub struct TaskAttrs {
    pub(crate) name: String,
    pub(crate) priority: i32,
    pub(crate) pool: String,
    pub(crate) max_attempts: u32,
    pub(crate) base_delay_ms: u64,
    pub(crate) backoff_multiplier: f64,
    pub(crate) max_delay_ms: u64,
    pub(crate) max_in_flight: u32,
    pub(crate) max_enqueue_per_second: u32,
    /// `None` = inherit runtime default; `Some("lwt"|"none")` overrides.
    pub(crate) idempotency_mode: Option<String>,
}

impl Parse for TaskAttrs {
    #[allow(clippy::too_many_lines)] // Attribute key/value parser with one arm per field.
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let first_key: Ident = input.parse()?;
        if first_key != "name" {
            return Err(syn::Error::new(
                first_key.span(),
                "expected `name` as the first attribute (e.g. `name = \"my_task\"`)",
            ));
        }
        let _: Token![=] = input.parse()?;
        let name_lit: LitStr = input.parse()?;
        let name = name_lit.value();
        let mut priority = 1i32;
        let mut pool = "global".to_string();
        let mut max_attempts = 3u32;
        let mut base_delay_ms = 1000u64;
        let mut backoff_multiplier = 2.0f64;
        let mut max_delay_ms = 30_000u64;
        let mut max_in_flight = 100u32;
        let mut max_enqueue_per_second = 50u32;
        let mut idempotency_mode: Option<String> = None;

        while input.peek(Token![,]) {
            let _: Token![,] = input.parse()?;
            if input.is_empty() {
                break;
            }
            let key: Ident = input.parse()?;
            let _: Token![=] = input.parse()?;
            match key.to_string().as_str() {
                "priority" => {
                    priority = input.parse::<LitInt>()?.base10_parse()?;
                }
                "pool" => {
                    let s: LitStr = input.parse()?;
                    pool = s.value();
                }
                "max_attempts" => {
                    max_attempts = input.parse::<LitInt>()?.base10_parse()?;
                }
                "base_delay_ms" => {
                    base_delay_ms = input.parse::<LitInt>()?.base10_parse()?;
                }
                "backoff_multiplier" => {
                    backoff_multiplier = input.parse::<LitFloat>()?.base10_parse()?;
                }
                "max_delay_ms" => {
                    max_delay_ms = input.parse::<LitInt>()?.base10_parse()?;
                }
                "max_in_flight" => {
                    max_in_flight = input.parse::<LitInt>()?.base10_parse()?;
                }
                "max_enqueue_per_second" => {
                    max_enqueue_per_second = input.parse::<LitInt>()?.base10_parse()?;
                }
                "idempotency_mode" => {
                    let s: LitStr = input.parse()?;
                    let v = s.value();
                    if v != "lwt" && v != "none" {
                        return Err(syn::Error::new(
                            s.span(),
                            "idempotency_mode must be \"lwt\" or \"none\"",
                        ));
                    }
                    idempotency_mode = Some(v);
                }
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("unknown boson::task attribute `{other}`"),
                    ));
                }
            }
        }

        Ok(Self {
            name,
            priority,
            pool,
            max_attempts,
            base_delay_ms,
            backoff_multiplier,
            max_delay_ms,
            max_in_flight,
            max_enqueue_per_second,
            idempotency_mode,
        })
    }
}
