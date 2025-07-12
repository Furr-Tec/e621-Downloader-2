
use std::time::Duration;

use anyhow::Result;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};

/// A builder that helps in making a new [ProgressStyle] for use.
pub(crate) struct ProgressStyleBuilder {
    /// The [ProgressStyle] being built.
    progress_style: ProgressStyle,
}

impl ProgressStyleBuilder {
    /// Sets the template of the progress style with terminal resize-safe formatting.
    ///
    /// # Arguments
    ///
    /// * `msg_template`: The template to use.
    ///
    /// returns: Result<ProgressStyleBuilder, anyhow::Error>
    pub(crate) fn template(mut self, msg_template: &str) -> Result<Self> {
        // Always use resize-safe templates to prevent text corruption during terminal resize
        let safe_template = Self::make_resize_safe_template(msg_template);
        
        match self.progress_style.clone().template(&safe_template) {
            Ok(style) => {
                self.progress_style = style;
                Ok(self)
            },
            Err(err) => {
                // Log the error and use a minimal fallback template
                warn!("Template error with '{}': {}. Using minimal fallback.", safe_template, err);
                
                // Use the most basic template that should always work
                let minimal_fallback = "{bar} {pos}/{len}";
                
                match self.progress_style.clone().template(minimal_fallback) {
                    Ok(style) => {
                        self.progress_style = style;
                        Ok(self)
                    },
                    Err(e) => {
                        // If even the minimal fallback fails, use default_bar
                        error!("Even minimal template failed: {}. Using default bar.", e);
                        self.progress_style = ProgressStyle::default_bar();
                        Ok(self)
                    }
                }
            }
        }
    }

    /// Makes a progress bar template resize-safe by using fixed-width elements and avoiding 
    /// problematic formatting that can break during terminal resize events.
    fn make_resize_safe_template(template: &str) -> String {
        // Replace potentially problematic template elements with resize-safe alternatives
        let mut safe_template = template.to_string();
        
        // Replace wide_bar with fixed-width bar to prevent layout issues
        safe_template = safe_template.replace("{wide_bar", "{bar:25");
        
        // Ensure bars have reasonable fixed widths to prevent overflow
        if !safe_template.contains("{bar:") && safe_template.contains("{bar}") {
            safe_template = safe_template.replace("{bar}", "{bar:20}");
        }
        
        // Replace complex message formatting with simpler alternatives
        if safe_template.len() > 100 {
            // If template is very long, use a simpler version
            return "{spinner} [{elapsed_precise}] {bar:25} {pos}/{len} - {msg}".to_string();
        }
        
        safe_template
    }

    /// Creates a basic ProgressStyleBuilder with resize-safe defaults
    /// This is a safer alternative when you just need a basic working style
    pub(crate) fn create_simple() -> Self {
        let mut builder = Self::default();
        // Use a resize-safe template with fixed widths
        let simple_template = "{spinner} [{elapsed_precise}] {bar:20} {pos}/{len}";
        
        match builder.progress_style.clone().template(simple_template) {
            Ok(style) => {
                builder.progress_style = style;
            },
            Err(e) => {
                error!("Failed to create simple template: {}. Using default bar.", e);
                builder.progress_style = ProgressStyle::default_bar();
            }
        }
        builder
    }

    /// Creates a minimal progress bar style that's extremely resistant to terminal resize issues
    pub(crate) fn create_minimal() -> Self {
        let mut builder = Self::default();
        // Use the most basic template possible that should never break
        let minimal_template = "{bar:15} {pos}/{len}";
        
        match builder.progress_style.clone().template(minimal_template) {
            Ok(style) => {
                builder.progress_style = style;
            },
            Err(_) => {
                // If even this fails, just use default
                builder.progress_style = ProgressStyle::default_bar();
            }
        }
        builder
    }

    /// Sets the progress style chars.
    ///
    /// # Arguments
    ///
    /// * `chars`: Progress chars to use.
    ///
    /// returns: Result<ProgressStyleBuilder, anyhow::Error>
    pub(crate) fn progress_chars(mut self, chars: &str) -> Result<Self> {
        self.progress_style = self.progress_style.progress_chars(chars);
        Ok(self)
    }

    pub(crate) fn build(self) -> ProgressStyle {
        self.progress_style
    }
}

impl Default for ProgressStyleBuilder {
    fn default() -> Self {
        Self {
            progress_style: ProgressStyle::default_bar(),
        }
    }
}

/// A builder that helps in initializing and configuring a new [ProgressBar] for use.
pub(crate) struct ProgressBarBuilder {
    /// The [ProgressBar] to build.
    pub(crate) progress_bar: ProgressBar,
}

impl ProgressBarBuilder {
    /// Creates new instance of the builder.
    ///
    /// # Arguments
    ///
    /// * `len`: Total length of the progress bar.
    ///
    /// returns: ProgressBarBuilder
    pub(crate) fn new(len: u64) -> Self {
        Self {
            progress_bar: ProgressBar::new(len),
        }
    }

    /// Sets the style of the progress bar to the style given.
    ///
    /// # Arguments
    ///
    /// * `progress_style`: The style to set the progress bar to.
    ///
    /// returns: ProgressBarBuilder
    pub(crate) fn style(self, progress_style: ProgressStyle) -> Self {
        self.progress_bar.set_style(progress_style);
        self
    }

    /// Sets the draw target (output) of the progress bar to the target given.
    /// Uses a lower refresh rate to reduce terminal resize issues.
    ///
    /// # Arguments
    ///
    /// * `target`: The output draw target.
    ///
    /// returns: ProgressBarBuilder
    pub(crate) fn draw_target(self, target: ProgressDrawTarget) -> Self {
        self.progress_bar.set_draw_target(target);
        self
    }

    /// Sets a resize-safe draw target with reduced refresh rate
    pub(crate) fn resize_safe_draw_target(self) -> Self {
        // Use stderr with lower refresh rate to reduce resize issues
        let target = ProgressDrawTarget::stderr_with_hz(2); // Only 2 updates per second
        self.progress_bar.set_draw_target(target);
        self
    }

    /// Resets the progress bar state to update it.
    pub(crate) fn reset(self) -> Self {
        self.progress_bar.reset();
        self
    }

    /// Sets the steady tick's duration to the given duration.
    ///
    /// # Arguments
    ///
    /// * `duration`: Steady tick duration.
    ///
    /// returns: ProgressBarBuilder
    pub(crate) fn steady_tick(self, duration: Duration) -> Self {
        self.progress_bar.enable_steady_tick(duration);
        self
    }

    /// Returns the newly built progress bar.
    pub(crate) fn build(self) -> ProgressBar {
        self.progress_bar
    }
}
