
#theme_set(theme_bw())

drop.unused <- function(df) {
  return(data.frame(lapply(df, function(x) if (is.factor(x)){ factor(x)} else {x})))
}

show_all <- function(infile, base=NULL, browse=FALSE) {
  benchmark <- sub(".csv", "", basename(infile), fixed=TRUE)
  df <- read.csv(infile)
  if (is.null(df$kind)) {
    df$kind <- "base"
  }
  
  if (!is.null(df$outlier)) {
    df <- subset(df, outlier != "YES")
  }

  # Grab serial infomation and then drop it from data frame
  df.serial <- subset(df, T == "serial")
  if (is.null(base)) {
    serial.means <- by(df.serial$LAST.TIME, df.serial$kind, mean)
    serial.time <- min(serial.means, na.rm=TRUE)
    serial.kind <- dimnames(serial.means)$`df.serial$kind`[match(serial.time, serial.means)]
    serial.its  <- mean(subset(df.serial, kind == serial.kind)$committed.its)
  } else {
    serial.kind <- base
    serial.time <- mean(subset(df.serial$LAST.TIME, df.serial$kind == base))
    serial.its <- mean(subset(df.serial$committed.its, df.serial$kind == base))
  }

  df <- drop.unused(subset(df, T != "serial"))
  Threads = as.numeric(as.character(df$T))

  # Print Summary
  best.parallel.time = min(by(df$LAST.TIME, df$kind, min, na.rm=TRUE))
  cat(sprintf("%s: best serial kind %s\n", infile, serial.kind))
  cat(sprintf("%s: serial time %f\n", infile, serial.time))
  cat(sprintf("%s: best // time %f\n", infile, best.parallel.time))
  cat(sprintf("%s: best speedup %f\n", infile, serial.time / best.parallel.time))

  # Throughput
  serial.tput = serial.its / serial.time
  d <- qplot(Threads, serial.its / LAST.TIME, data=df, color=kind, shape=kind, geom=c("point", "smooth"), se=FALSE, ymin=0) +
    scale_y_continuous("Throughput (it/ms)") +
    geom_abline(color = "black", slope=serial.tput) +
    ylim(0, max(Threads) * serial.tput)
  d + opts(legend.position = "none")
  ggsave(file = sprintf("%s.pdf", benchmark), width=3, height=3, pointsize=14)

  # Separate legend
  d + opts(keep="legend_box")
  ggsave(file = sprintf("%s-legend.pdf", benchmark), width=3, height=3, pointsize=14)

  # Time
  qplot(Threads, LAST.TIME, data=df, color=kind, shape=kind, geom=c("point"), ymin=0) +
    stat_function(fun=function(x) {serial.time/x}, color="black") +
    opts(legend.position = "none")
  ggsave(file = sprintf("%s-time.pdf", benchmark), width=3, height=3, pointsize=14)

  # Various other plots
  df$IDLE.TIME = df$IDLE.THREAD.TIME / Threads
  ymax <- best.parallel.time #max(max(df$IDLE.TIME, df$GC.TIME, df$SERIAL.TIME, na.rm=TRUE), best.parallel.time)
  rest.ylim = c(0, ymax)
  pdf(sprintf("%s-rest.pdf", benchmark), height=3, width=7)
  grid.newpage()
  pushViewport(viewport(layout=grid.layout(nrow=1, ncol=4)))
  a <- qplot(Threads, ABORT.RATIO, data=df, color=kind, shape=kind, ymin=0, geom=c("point")) +
    scale_x_continuous("") + opts(legend.position = "none")
  b <- qplot(Threads, IDLE.TIME, data=df, ylim=rest.ylim, color=kind, shape=kind, geom=c("point")) +
    scale_x_continuous("") + opts(legend.position = "none")
  c <- qplot(Threads, GC.TIME, data=df, ylim=rest.ylim, color=kind, shape=kind, geom=c("point")) +
    scale_x_continuous("") + opts(legend.position = "none")
  d <- qplot(Threads, SERIAL.TIME, data=df, ylim=rest.ylim, color=kind, shape=kind, geom=c("point")) +
    scale_x_continuous("") + opts(legend.position = "none")
  print(a, vp=viewport(layout.pos.row=1, layout.pos.col=1))
  print(b, vp=viewport(layout.pos.row=1, layout.pos.col=2))
  print(c, vp=viewport(layout.pos.row=1, layout.pos.col=3))
  print(d, vp=viewport(layout.pos.row=1, layout.pos.col=4))
  dev.off()

  # Relative versions of other plots
  rest.ylim = c(0, 1)
  pdf(sprintf("%s-rest-rel.pdf", benchmark), height=3, width=7)
  grid.newpage()
  pushViewport(viewport(layout=grid.layout(nrow=1, ncol=4)))
  b <- qplot(Threads, IDLE.TIME / LAST.TIME, data=df, ylim=rest.ylim, color=kind, shape=kind, geom=c("point")) +
    scale_x_continuous("") + opts(legend.position = "none")
  c <- qplot(Threads, GC.TIME / LAST.TIME, data=df, ylim=rest.ylim, color=kind, shape=kind, geom=c("point")) +
    scale_x_continuous("") + opts(legend.position = "none")
  d <- qplot(Threads, SERIAL.TIME / LAST.TIME, data=df, ylim=rest.ylim, color=kind, shape=kind, geom=c("point")) +
    scale_x_continuous("") + opts(legend.position = "none")
  print(b, vp=viewport(layout.pos.row=1, layout.pos.col=2))
  print(c, vp=viewport(layout.pos.row=1, layout.pos.col=3))
  print(d, vp=viewport(layout.pos.row=1, layout.pos.col=4))
  dev.off()

  # Plot model error
  t1 <- subset(df, df$T == 1)
  #   copy corresponding t=1 stats over all rows
  t1.times <- by(t1$LAST.TIME, t1$kind, mean)[df$kind] 
  t1p.times <- by(t1$LAST.TIME - t1$GC.TIME, t1$kind, mean)[df$kind] 
  t1.its <- by(t1$total.its, t1$kind, mean)[df$kind]
  #   form intermediate values
  serial.overhead <- df$IDLE.THREAD.TIME / Threads + df$SERIAL.TIME + df$GC.TIME
  parallel.time <- t1p.times - serial.overhead
  predicted <- parallel.time * (df$total.its / t1.its) / Threads + serial.overhead
  rel.m0 <- with(df, (LAST.TIME - t1p.times / Threads) / LAST.TIME)
  rel.m1 <- with(df, (LAST.TIME - predicted) / LAST.TIME)
  X <- data.frame(T=rep(Threads, 2), Relative.Error=c(rel.m0, rel.m1), kind=rep(df$kind, 2), case=rep(c("Naive", "Simple"), each=length(Threads)))
  qplot(as.factor(Threads), Relative.Error, data=X, facets=. ~ case, geom="blank") +
    geom_boxplot() +
    scale_x_discrete("")  +
    opts(legend.position = "none")
  ggsave(file = sprintf("%s-model.pdf", benchmark), width=6, height=3, pointsize=14)

  # Plot scalability
  qplot(Threads, t1.times / LAST.TIME, data=df, color=kind, shape=kind, geom=c("point", "smooth"), se=FALSE) +
    scale_x_continuous("") + opts(legend.position = "none") +
    stat_function(fun=function(x) {x}, color="black")
  ggsave(file = sprintf("%s-scale.pdf", benchmark), width=3, height=3, pointsize=14)

  if (browse) {
    browser()
  }
}

args <- commandArgs(trailingOnly=TRUE)

if (length(args) < 1) {
  cat("usage: Rscript report.R <report.csv> [basekind] [browse?]\n")
  quit(save = "no", status=1)
} 

library(ggplot2)
if (length(args) == 1) {
  show_all(args[1])
} else if (length(args) == 2) {
  show_all(args[1], base=args[2])
} else {
  show_all(args[1], base=args[2], browse=TRUE)
}

