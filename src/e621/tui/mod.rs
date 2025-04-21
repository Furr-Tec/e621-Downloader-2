/*
 * Copyright (c) 2022 McSib
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


use std::time::Duration;

use anyhow::Result;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};

/// A builder that helps in making a new [ProgressStyle] for use.
pub(crate) struct ProgressStyleBuilder {
    /// The [ProgressStyle] being built.
    progress_style: ProgressStyle,
}

impl ProgressStyleBuilder {
    /// Sets the template of the progress style.
    ///
    /// # Arguments
    ///
    /// * `msg_template`: The template to use.
    ///
    /// returns: Result<ProgressStyleBuilder, anyhow::Error>
    pub(crate) fn template(mut self, msg_template: &str) -> Result<Self> {
        match self.progress_style.clone().template(msg_template) {
            Ok(style) => {
                self.progress_style = style;
                Ok(self)
            },
            Err(err) => {
                // Log the error and attempt to use a simpler template as fallback
                warn!("Template error with '{}': {}. Using fallback template.", msg_template, err);
                // Simplified fallback template without problematic fields
                let fallback = "{spinner} [{elapsed_precise}] {bar} {pos}/{len}";
                
                match self.progress_style.clone().template(fallback) {
                    Ok(style) => {
                        self.progress_style = style;
                        Ok(self)
                    },
                    Err(e) => {
                        // If even the fallback fails, use default_bar
                        error!("Fallback template also failed: {}. Using default bar.", e);
                        self.progress_style = ProgressStyle::default_bar();
                        Ok(self)
                    }
                }
            }
        }
    }

    /// Creates a basic ProgressStyleBuilder with sensible defaults
    /// This is a safer alternative when you just need a basic working style
    pub(crate) fn create_simple() -> Self {
        let mut builder = Self::default();
        // Use a very basic template without any problematic fields
        match builder.progress_style.clone().template("{spinner} [{elapsed_precise}] {bar} {pos}/{len}") {
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
