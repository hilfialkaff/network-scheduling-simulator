document = main
SOURCES = $(wildcard *.tex) $(document).bib

all: $(document).pdf

pdf : $(document).pdf

$(document).pdf : $(SOURCES)
	pdflatex $(document)
	bibtex $(document)
	pdflatex $(document)
	pdflatex $(document)
	rm -f *.aux *.bbl *.log *.blg *.lot *.lof *.out

clean:
	rm -f *.aux *.bbl *.log *.blg *.lot *.lof *.out $(document).pdf
