# Galois, a framework to exploit amorphous data-parallelism in irregular
# programs.
# 
# Copyright (C) 2009 Intelligent Software Systems group, University of Texas at Austin.
# 
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
# 
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA.
# 
# 

# Sample command line:
#   R --vanilla --args stats-merged.txt ... < "scripts/parameter.R"
# To debug, run
#   R --vanilla --args ...
#   # Insert calls to browser() to cause execution to break at that point
#   > source("scripts/parameter.R")


# Maximum number of time steps to plot for boxplots, if the number of time
# steps exceeds this, plot MAX_TS equally distant time steps instead
MAX_BOXPLOT_TS = 75
MAX_TS = 10000

# overlay the available parallelism plot
overlay_ap = function(ap, fn, ylab, main="", col="blue") {
  par(mar=c(5,4,4,4))
  do_ap(ap, main, axiscol="blue")
  par(new=T) 
  fn()
  axis(side=4)
  mtext(ylab, side=4, line=2)
}

# plot available parallelism
do_ap = function(ap, main, axiscol="black", log="", avgpar=F) {
  if (log == "x") {
    xlab = "Computation Step (log-scale)"
  } else {
    xlab = "Computation Step"
  }

  plot(ap, col="blue", type="p", xlab=xlab, ylab="", main=main, log=log)
  lines(ap, col="blue", lwd=5)
  if (avgpar) {
    # print average parallelism: total available parallelism / computation steps 
    abline(h=sum(ap) / length(ap))
  }
  mtext("Available Parallelism", side=2, line=2, col=axiscol)
}

# plot showing min, max and mean of neighborhood sizes 
do_minmax = function(ap, values) {
  ylab = "Neighborhood Size"
  main = "Min, max and mean of neighborhood sizes"
  mins = values[, "Min.Neigh"]
  maxs = values[, "Max.Neigh"]
  means = values[, "Ave.Neigh"]
  least = min(mins)
  great = max(maxs)

  fn = function() {
    plot(means, axes=F, ylim=c(least,great), xlab="", ylab="", col="transparent")
    lines(means, col="black")
    lines(mins, col="purple")
    lines(maxs, col="red")
    legend("topright",
         c("Max", "Mean", "Min"),
         fill=c("red", "black", "purple"))
  }

  overlay_ap(ap, fn, ylab, main)
}

# plot normalized neighborhoods (1/ number active nodes)
do_normneigh = function(ap, neigh) {
  active = t(as.matrix(ap))
  norm_neigh = t(rep(1,length(active)) / active)
  overlay_ap(ap, function() { 
      plot(norm_neigh, axes=F, xlab="", ylab="")
    }, "Mean Normalized Neighborhood Size", "Neighborhood sizes relative to total size")
}

# available parallelism, worklist size, parallelism intensity
do_intensity = function(ap, wl, notuseful) {
  newwork = vector(length=length(wl))

  # New work is the current worklist size minus the items executed in
  # previous time step and minus the size of the previous worklist
  if (length(wl) > 1) {
    newwork[1] = 0
    for (index in 2:length(wl)) {
      newwork[index] = wl[index] - (wl[index-1] - ap[index-1])
    }
  }
  par(mar=c(5,4,4,4))
#  barplot(rbind(wl - newwork, notuseful, newwork),
#    names.arg=1:length(wl),
#    xlab="Computation Step",
#    ylab="", 
#    main="Worklist sizes and parallelism intensity",
#    legend.text=c("original work", "not useful", "new work"))
  plot(wl,
       xlab="Computation Step",
       main="Worklist sizes and parallelism intensity",
       ylab="", col="transparent")
  lines(ap, col="blue", lwd=5)
  lines(notuseful, col="red")
  lines(newwork, col="purple")
  lines(wl, col="black")
  legend("topright",
         c("Worklist Size", "Available Parallelism", "Not Useful Work", "New Work"),
         fill=c("black", "blue", "red", "purple"))
  mtext("Active Nodes", side=2, line=2, col="black")

  par(new=T)

  plot(ap / wl, ylim=c(0, 1), axes=F, xlab="", ylab="", col="green")
  axis(side=4)
  mtext("Parallelism Intensity", side=4, line=2, col="green")
}

# Deterministically exponentially drop values from a vector
# Keep the first 10, one out of the next 10, one out of the next 100, etc...
shrink_exp = function(v, first=100, size=1000) {
  len = length(v)
  mask = vector(length=len)
  mask[1:first] = T

  prob = sort(rlnorm(len - first), decreasing=T)
  mask[sample((first+1):len, size=size, prob=prob)] = T

  v[!mask] = NA

  return(v)
}

do_long_ap = function(ap, main) {
  #do_ap(shrink_exp(ap), main, axiscol="blue", log="x", avgpar=T)
  do_ap(shrink_exp(ap), main, axiscol="blue", avgpar=T)
}

do_graphs = function(output, data_summary) {
  pdf(output)
  ap = data_summary[,"Active.Nodes"]
  if (length(ap) > MAX_TS) {
    do_long_ap(ap, "Available parallelism")
  } else {
    do_ap(ap, "Available parallelism", axiscol="blue", avgpar=T)
  }
  wl = data_summary[,"Worklist.Size"]
  notuseful = data_summary[,"Not.Useful.Work.Size"]
  do_intensity(ap, wl, notuseful)
  do_minmax(ap, data_summary)
  dev.off()
}

args = commandArgs(trailingOnly=T)
output = args[1]
data_summary = NULL
neigh = NULL

# Compute total statistics
for (arg in args[2:length(args)]) {
  data_summary = rbind(data_summary, read.csv(arg))
  do_graphs(output, data_summary)
}
