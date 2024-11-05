use convert_case::{Case, Casing};
use darling::FromDeriveInput;
use proc_macro2::TokenStream;
use quote::{quote, quote_spanned, ToTokens};
use syn::{spanned::Spanned, DeriveInput, Fields, FieldsNamed};

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
    tag: Option<String>,
}

fn bigquery_macro_impl(input: proc_macro::TokenStream) -> syn::Result<TokenStream> {
    let ast: DeriveInput = syn::parse(input)?;
    let args = BigQueryArgs::from_derive_input(&ast)?;
    let root_name = &args.ident;

    match &ast.data {
        syn::Data::Struct(s) => match &s.fields {
            Fields::Named(fields) => Ok(impl_wrapper(
                root_name,
                handle_named_fields(fields, "name", args.tag.as_deref())?,
            )),
            _ => Err(syn::Error::new(
                ast.ident.span(),
                "Unsupported struct type to derive BigQuerySchema",
            )),
        },
        syn::Data::Enum(e) => {
            // If all fields are unit type, the type is just a string.
            if e.variants.iter().all(|v| matches!(v.fields, Fields::Unit)) {
                return Ok(impl_wrapper(
                    root_name,
                    quote!(TableFieldSchema::string(name)),
                ));
            }

            // If any but not all fields are unit type, the type could either be string or struct.
            // Let's just use JSON.
            if e.variants.iter().any(|v| matches!(v.fields, Fields::Unit)) {
                return Ok(impl_wrapper(
                    root_name,
                    quote!(TableFieldSchema::json(name)),
                ));
            }

            let field_vals = e.variants.iter().map(|variant| {
                let snake_ident = variant.ident.to_string().to_case(Case::Snake);
                Ok(match &variant.fields {
                    // Single unnamed field is serialized as structs snake_case variants for keys,
                    // and the fields' own serialization for the values.
                    Fields::Unnamed(fields) => {
                        let mut iter = fields.unnamed.iter();
                        let (Some(f), None) = (iter.next(), iter.next()) else {
                            return 
                                Err(syn::Error::new(
                                    variant.ident.span(),
                                    "Tuple enums variants must have a single field to derive BigQuerySchema",
                                ));
                        };
                        let ty = &f.ty;
                        quote!(<#ty>::big_query_schema(#snake_ident))
                    }
                    // Named fields are serialized the same as if the named fields were in their
                    // own struct.
                    Fields::Named(fields) => handle_named_fields(fields, snake_ident, None)?,
                    Fields::Unit => unreachable!(),
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
        _ => Err(syn::Error::new(
            ast.ident.span(),
            "Unsupported type to derive BigQuerySchema",
        )),
    }
}

fn handle_named_fields<T: ToTokens>(
    fields: &FieldsNamed,
    name: T,
    tag: Option<&str>,
) -> syn::Result<TokenStream> {
    let field_vals = fields
        .named
        .iter()
        .map(|field| {
            let ident = if let Some(attr) = field
                .attrs
                .iter()
                .find(|attr| attr.meta.path().is_ident("bigquery"))
            {
                let f = &attr.meta.require_name_value()?.value;
                quote!(stringify!(#f))
            } else {
                let f = field
                    .ident
                    .as_ref()
                    .unwrap()
                    .to_string()
                    .to_case(Case::Snake);
                quote!(#f)
            };
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
