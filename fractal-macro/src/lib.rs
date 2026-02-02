use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{FnArg, Ident, ItemFn, Pat, ReturnType, parse_macro_input};

#[proc_macro_attribute]
pub fn task_op(_: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);
    let vis = func.vis;
    let sig = func.sig;
    let block = func.block;
    let output = match &sig.output {
        ReturnType::Default => quote! { () },
        ReturnType::Type(_, ty) => quote! { #ty },
    };
    let name: Ident = sig.ident;
    let mut param_types = vec![];
    let mut param_names = vec![];
    sig.inputs.iter().for_each(|item| match item {
        FnArg::Typed(pt) => match &*pt.pat {
            Pat::Ident(ident) => {
                param_names.push(ident);
                param_types.push(pt.ty.clone());
            }
            _ => (),
        },
        FnArg::Receiver(_) => (),
    });
    let struct_name = format_ident!("Task{}", name);
    let expanded = quote! {
        #vis struct #struct_name {
            #(#param_names : RawTaskRef),*
        }
        impl #struct_name {
            pub fn new(#(#param_names : RawTaskRef),*) -> Self {
                Self {#(#param_names),*}
            }
        }
        #vis fn #name (#(#param_names : RawTaskRef),*) -> #struct_name {
            #struct_name::new(#(#param_names),*)
        }
        impl #struct_name {
            fn _run(#(#param_names: #param_types),*) -> #output #block
        }
        impl Task for #struct_name {
            type Output = #output;
            fn compute(self, scheduler: &mut SubScheduler) -> Self::Output {
                #(
                    let #param_names = scheduler.get(&self.#param_names).unwrap();
                )*
                Self::_run(#(#param_names.read().unwrap().downcast_ref().unwrap()),*)
            }
        }
        impl CompleteTask for #struct_name {
            type Output = #output;
            fn dependency(&self) -> std::collections::HashSet<RawTaskRef> {
                let mut result = std::collections::HashSet::new();
                #(result.insert(self.#param_names);)*
                result
            }
            fn task(self) -> impl Task<Output=Self::Output> {
                self
            }
        }
    };
    println!("{}", expanded.to_string());
    TokenStream::from(expanded)
}
