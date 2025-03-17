use convert_case::{Case, Casing};
use darling::{ast::{Data, Fields, Style}, util::SpannedValue, FromDeriveInput, FromField, FromVariant};
use proc_macro2::{Span, TokenStream};
use quote::{quote, quote_spanned, ToTokens};
use syn::{DeriveInput, Ident, Type};

#[proc_macro_derive(Record, attributes(record))]
pub fn record_macro_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    record_macro_impl(input)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(record))]
struct RecordArgs {
    ident: syn::Ident,
    rename: Option<String>,
}

fn record_macro_impl(input: proc_macro::TokenStream) -> syn::Result<TokenStream> {
    let ast: DeriveInput = syn::parse(input).unwrap();
    let args = RecordArgs::from_derive_input(&ast)?;
    let name = &args.ident;
    let snake_name = args.rename.clone().unwrap_or_else(|| args.ident.to_string()).to_case(Case::Snake);

    Ok(quote! {
        impl crate::record::Record for #name {
            fn table_name() -> &'static str {
                #snake_name
            }
        }
    }
    .into())
}

#[proc_macro_derive(BigQuerySchema, attributes(bigquery))]
pub fn bigquery_macro_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    bigquery_macro_impl(input)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(bigquery))]
struct BigQueryArgs {
    ident: syn::Ident,
    data: Data<BigQueryVariant, SpannedValue<BigQueryField>>,

    tag: Option<String>,
}

#[derive(Debug, FromVariant)]
#[darling(attributes(bigquery))]
struct BigQueryVariant {
    ident: Ident,
    fields: Fields<SpannedValue<BigQueryField>>,
    #[darling(default)]
    skip: bool,
}

#[derive(Debug, FromField)]
#[darling(attributes(bigquery))]
struct BigQueryField {
    ident: Option<Ident>,
    ty: Type,

    rename: Option<String>
}

fn bigquery_macro_impl(input: proc_macro::TokenStream) -> syn::Result<TokenStream> {
    let ast: DeriveInput = syn::parse(input)?;
    let args = BigQueryArgs::from_derive_input(&ast)?;
    let root_name = &args.ident;

    match &args.data {
        Data::Struct(s) => {
            let Style::Struct = s.style else {
                return Err(syn::Error::new(
                    ast.ident.span(),
                    "Unsupported struct type to derive BigQuerySchema",
                ));
            };
            Ok(impl_wrapper(
                root_name,
                handle_named_fields(&s, syn::Ident::new("name", Span::call_site()), args.tag.as_deref())?,
            ))
        },
        Data::Enum(variants) => {
            // If all fields are unit type, the type is just a string.
            if variants.iter().all(|v| matches!(v.fields.style, Style::Unit)) {
                return Ok(impl_wrapper(
                    root_name,
                    quote!(TableFieldSchema::string(name)),
                ));
            }

            // If any but not all fields are unit type, the type could either be string or struct.
            // Let's just use JSON.
            if variants.iter().any(|v| matches!(v.fields.style, Style::Unit)) {
                return Ok(impl_wrapper(
                    root_name,
                    quote!(TableFieldSchema::json(name)),
                ));
            }

            let field_vals = variants.iter().map(|variant| {
                let snake_ident = variant.ident.to_string().to_case(Case::Snake);
                Ok(match &variant.fields.style {
                    // Single unnamed field is serialized as structs snake_case variants for keys,
                    // and the fields' own serialization for the values.
                    Style::Tuple => {
                        let mut iter = variant.fields.fields.iter();
                        let (Some(f), None) = (iter.next(), iter.next()) else {
                            return 
                                Err(syn::Error::new(
                                    variant.ident.span(),
                                    "Tuple enum variants must have a single field to derive BigQuerySchema",
                                ));
                        };
                        let ty = &f.ty;
                        quote!(<#ty>::big_query_schema(#snake_ident))
                    }
                    // Named fields are serialized the same as if the named fields were in their
                    // own struct.
                    Style::Struct => handle_named_fields(&variant.fields, snake_ident, None)?,
                    Style::Unit => unreachable!(),
                })
            })
            .chain(args.tag.into_iter().map(|tag| Ok(quote!(TableFieldSchema::string(#tag)))))
            .collect::<syn::Result<Vec<_>>>()?;

            Ok(impl_wrapper(
                root_name,
                quote! {
                    TableFieldSchema::record(name, vec![
                        #(#field_vals),*
                    ])
                },
            ))
        }
    }
}

fn bigquery_prost(args: BigQueryArgs) {

}

fn handle_named_fields<T: ToTokens>(
    fields: &Fields<SpannedValue<BigQueryField>>,
    name: T,
    tag: Option<&str>,
) -> syn::Result<TokenStream> {
    let field_vals = fields
        .iter()
        .map(|field| {
            let ident = field.rename.clone().unwrap_or_else(|| field
                .ident
                .as_ref()
                .unwrap()
                .to_string()
                .to_case(Case::Snake));
            let ty = &field.ty;
            Ok(quote_spanned!(field.span() => <#ty>::big_query_schema(#ident)))
        })
        .chain(
            tag.into_iter()
                .map(|tag| Ok(quote!(TableFieldSchema::string(#tag)))),
        )
        .collect::<syn::Result<Vec<_>>>()?;
    Ok(quote! {
        TableFieldSchema::record(#name, vec![
            #(#field_vals),*
        ])
    }
    .into())
}

fn impl_wrapper<N, C>(root_name: N, contents: C) -> TokenStream
where
    N: ToTokens,
    C: ToTokens,
{
    quote! {
        impl crate::record::BigQuerySchema for #root_name {
            fn big_query_schema(name: &str) -> gcp_bigquery_client::model::table_field_schema::TableFieldSchema {
                use crate::record::BigQuerySchema;
                use ::gcp_bigquery_client::model::table_field_schema::TableFieldSchema;
                #contents
            }
        }
    }
}
